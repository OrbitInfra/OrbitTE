/* Callback controls prove identity joining and pass-through, not native DMA. */
#include "cq_fault.c"
#include <assert.h>
static int post_calls, modify_calls, poll_calls;
static struct ibv_wc completion;
static int fake_post(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad) {
    (void)qp; (void)wr; (void)bad; ++post_calls; return 0;
}
static int fake_modify(struct ibv_qp *qp, struct ibv_qp_attr *attr, int mask) {
    assert(qp->qp_num == 7 && attr->qp_state == IBV_QPS_ERR && mask == IBV_QP_STATE);
    ++modify_calls; return 0;
}
static int fake_poll(struct ibv_cq *cq, int capacity, struct ibv_wc *wc) {
    (void)cq; assert(capacity > 0); ++poll_calls; *wc = completion; return 1;
}
int main(void) {
    struct ibv_context context[2] = {0};
    for (int i = 0; i < 2; ++i) {
        context[i].ops.poll_cq = fake_poll; context[i].ops.post_send = fake_post;
        attach(&context[i]);
    }
    native_modify_qp = fake_modify;
    struct ibv_cq cq = {.context = &context[0]};
    struct ibv_qp qp = {.context = &context[0], .send_cq = &cq, .qp_num = 7};
    struct ibv_sge sge = {.addr = 1024, .length = 4096};
    struct ibv_send_wr wr = {.wr_id = 9, .sg_list = &sge, .num_sge = 1, .opcode = IBV_WR_RDMA_READ};
    struct ibv_send_wr *bad = NULL;
    struct inflight_snapshot snapshot;
    assert(sizeof(snapshot) == 88);
    assert(orbitte_inflight_fault(1, 0, &snapshot) == EINVAL);
    assert(orbitte_inflight_fault(1, 1, &snapshot) == 0);
    assert(orbitte_inflight_fault(1, 1, &snapshot) == EINVAL);
    assert(orbitte_inflight_fault(2, 0, &snapshot) == EINVAL);
    struct snapshot other;
    assert(orbitte_cq_fault(1, 0, 1, 1, &other) == EINVAL);
    assert(post(&qp, &wr, &bad) == 0 && post_calls == 1 && modify_calls == 1);
    struct ibv_wc received;
    completion = (struct ibv_wc){.wr_id = 8, .qp_num = 7, .status = IBV_WC_WR_FLUSH_ERR};
    assert(poll(&cq, 1, &received) == 1 && received.wr_id == 8);
    completion.wr_id = 9; completion.qp_num = 6;
    assert(poll(&cq, 1, &received) == 1 && received.qp_num == 6);
    /* A failed WC with an invalid opcode still joins its WR. */
    completion.qp_num = 7; completion.opcode = (enum ibv_wc_opcode)0x7fffffff;
    assert(poll(&cq, 1, &received) == 1 && received.status == IBV_WC_WR_FLUSH_ERR);
    assert(orbitte_inflight_fault(0, 0, &snapshot) == 0 && snapshot.failed == 1 && snapshot.completed == 0);
    assert(orbitte_inflight_fault(2, 0, &snapshot) == EINVAL); /* destruction still absent */
    inflight_destroyed(&cq, 6, 0);
    assert(orbitte_inflight_fault(2, 0, &snapshot) == EINVAL);
    inflight_destroyed(&cq, 7, 0);
    assert(orbitte_inflight_fault(2, 0, &snapshot) == 0 && snapshot.armed == 0 && snapshot.destroyed == 1);
    assert(poll_calls == 3 && state.lost == 0);
    puts("In-flight controls passed: exact WR/QP, real callback once, unchanged WC, destruction fence");
}
