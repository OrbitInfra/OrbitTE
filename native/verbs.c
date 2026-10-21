/* System libibverbs ABI bridge. Engine, protocol and backing ownership are Rust. */
#define _GNU_SOURCE
#include <errno.h>
#include <infiniband/verbs.h>
#include <stdint.h>
#include <string.h>

struct ote_handles {
    struct ibv_context *context;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_mr *mr;
};
struct ote_desc {
    uint64_t address;
    uint32_t rkey, qpn, psn;
    uint16_t lid;
    uint8_t mtu, link_layer;
    uint8_t gid[16];
};
struct ote_completion {
    uint64_t id;
    uint32_t status, opcode, length, vendor_error;
};
static int failure(void) { return errno ? errno : EIO; }

int ote_verbs_open(struct ote_handles *h, const char *name, uint8_t port,
                   uint8_t gid_index, void *buffer, size_t length,
                   uint32_t permissions, uint32_t psn, struct ote_desc *local) {
    int count = 0;
    struct ibv_device **devices = ibv_get_device_list(&count);
    if (!devices) return failure();
    int open_error = ENODEV;
    for (int i = 0; i < count; i++) {
        if (!strcmp(name, ibv_get_device_name(devices[i]))) {
            h->context = ibv_open_device(devices[i]);
            open_error = h->context ? 0 : failure();
            break;
        }
    }
    ibv_free_device_list(devices);
    if (open_error) return open_error;
    struct ibv_port_attr attr = {0};
    int error = ibv_query_port(h->context, port, &attr);
    if (error) return error;
    if (attr.state != IBV_PORT_ACTIVE) return ENETDOWN;
    union ibv_gid gid = {0};
    error = ibv_query_gid(h->context, port, gid_index, &gid);
    if (error) return error;
    if (attr.link_layer == IBV_LINK_LAYER_ETHERNET) {
        const uint8_t zero[16] = {0};
        if (!memcmp(gid.raw, zero, sizeof zero)) return EADDRNOTAVAIL;
    }
    h->pd = ibv_alloc_pd(h->context);
    if (!h->pd) return failure();
    h->cq = ibv_create_cq(h->context, 4, NULL, NULL, 0);
    if (!h->cq) return failure();
    int access = IBV_ACCESS_LOCAL_WRITE;
    if (permissions & 1) access |= IBV_ACCESS_REMOTE_READ;
    if (permissions & 2) access |= IBV_ACCESS_REMOTE_WRITE;
    h->mr = ibv_reg_mr(h->pd, buffer, length, access);
    if (!h->mr) return failure();
    struct ibv_qp_init_attr init = {0};
    init.send_cq = h->cq;
    init.recv_cq = h->cq;
    init.qp_type = IBV_QPT_RC;
    init.sq_sig_all = 1;
    init.cap.max_send_wr = 1;
    init.cap.max_recv_wr = 1;
    init.cap.max_send_sge = 1;
    init.cap.max_recv_sge = 1;
    h->qp = ibv_create_qp(h->pd, &init);
    if (!h->qp) return failure();
    struct ibv_qp_attr qp_attr = {0};
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.pkey_index = 0;
    qp_attr.port_num = port;
    qp_attr.qp_access_flags = access & ~IBV_ACCESS_LOCAL_WRITE;
    error = ibv_modify_qp(h->qp, &qp_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                                              IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (error) return error;
    local->address = (uint64_t)(uintptr_t)buffer;
    local->rkey = h->mr->rkey;
    local->qpn = h->qp->qp_num;
    local->psn = psn;
    local->lid = attr.lid;
    local->mtu = attr.active_mtu;
    local->link_layer = attr.link_layer;
    memcpy(local->gid, gid.raw, sizeof local->gid);
    return 0;
}

int ote_verbs_connect(struct ote_handles *h, const struct ote_desc *local,
                      const struct ote_desc *remote, uint8_t port,
                      uint8_t gid_index, uint8_t traffic_class, uint8_t hop_limit) {
    struct ibv_qp_attr attr = {0};
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = local->mtu < remote->mtu ? local->mtu : remote->mtu;
    attr.dest_qp_num = remote->qpn;
    attr.rq_psn = remote->psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.dlid = remote->lid;
    attr.ah_attr.port_num = port;
    const uint8_t zero[16] = {0};
    if (local->link_layer == IBV_LINK_LAYER_ETHERNET ||
        memcmp(remote->gid, zero, sizeof zero)) {
        attr.ah_attr.is_global = 1;
        memcpy(attr.ah_attr.grh.dgid.raw, remote->gid, sizeof remote->gid);
        attr.ah_attr.grh.sgid_index = gid_index;
        attr.ah_attr.grh.hop_limit = hop_limit;
        attr.ah_attr.grh.traffic_class = traffic_class;
    }
    int error = ibv_modify_qp(h->qp, &attr, IBV_QP_STATE | IBV_QP_AV |
        IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
        IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    if (error) return error;
    memset(&attr, 0, sizeof attr);
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = local->psn;
    attr.max_rd_atomic = 1;
    return ibv_modify_qp(h->qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT |
        IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
}

int ote_verbs_post(struct ote_handles *h, uint64_t remote_address, uint32_t rkey,
                   uint32_t length, int write) {
    struct ibv_sge sge = {0};
    sge.addr = (uint64_t)(uintptr_t)h->mr->addr;
    sge.length = length;
    sge.lkey = h->mr->lkey;
    struct ibv_send_wr wr = {0}, *bad = NULL;
    wr.wr_id = 1;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = write ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote_address;
    wr.wr.rdma.rkey = rkey;
    return ibv_post_send(h->qp, &wr, &bad);
}
int ote_verbs_poll(struct ote_handles *h, struct ote_completion *result) {
    struct ibv_wc completion = {0};
    int count = ibv_poll_cq(h->cq, 1, &completion);
    if (count == 1) {
        result->id = completion.wr_id;
        result->status = completion.status;
        result->opcode = completion.opcode;
        result->length = completion.byte_len;
        result->vendor_error = completion.vendor_err;
    }
    return count;
}
int ote_verbs_destroy_qp(struct ote_handles *h) {
    if (!h->qp) return 0;
    int error = ibv_destroy_qp(h->qp);
    if (!error) h->qp = NULL;
    return error;
}
int ote_verbs_deregister(struct ote_handles *h) {
    if (!h->mr) return 0;
    int error = ibv_dereg_mr(h->mr);
    if (!error) h->mr = NULL;
    return error;
}
int ote_verbs_destroy_cq(struct ote_handles *h) {
    if (!h->cq) return 0;
    int error = ibv_destroy_cq(h->cq);
    if (!error) h->cq = NULL;
    return error;
}
int ote_verbs_dealloc_pd(struct ote_handles *h) {
    if (!h->pd) return 0;
    int error = ibv_dealloc_pd(h->pd);
    if (!error) h->pd = NULL;
    return error;
}
int ote_verbs_close_device(struct ote_handles *h) {
    if (!h->context) return 0;
    int error = ibv_close_device(h->context);
    if (!error) h->context = NULL;
    return error;
}
