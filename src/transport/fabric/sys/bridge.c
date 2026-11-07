#define _POSIX_C_SOURCE 200809L
#include "bridge.h"
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

_Static_assert(sizeof(struct fi_context2) <= 8 * sizeof(uintptr_t), "fabric context storage");
_Static_assert(_Alignof(struct fi_context2) <= _Alignof(uintptr_t), "fabric context alignment");

typedef struct fi_info *(*dup_fn)(const struct fi_info *);
typedef void (*free_fn)(struct fi_info *);
typedef int (*get_fn)(uint32_t, const char *, const char *, uint64_t,
                      const struct fi_info *, struct fi_info **);
typedef int (*fabric_fn)(struct fi_fabric_attr *, struct fid_fabric **, void *);
typedef uint32_t (*version_fn)(void);

int orbite_fi_discover(const struct orbite_fi_api *a, int tcp,
                       const char *fabric_name, void **out) {
    struct fi_info *h = ((dup_fn)a->dupinfo)(NULL);
    if (!h) return -FI_ENOMEM;
    h->caps = FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    h->mode = FI_CONTEXT | FI_CONTEXT2;
    h->ep_attr->type = FI_EP_RDM;
    h->domain_attr->threading = FI_THREAD_SAFE;
    /* A source release receipt requires visibility at the target, not merely
       transmission. Providers unable to guarantee it must fail discovery. */
    h->tx_attr->op_flags = FI_DELIVERY_COMPLETE;
    h->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY;
    if (tcp) h->domain_attr->mr_mode |= FI_MR_ENDPOINT;
    h->fabric_attr->prov_name = strdup(tcp ? "tcp;ofi_rxm" : "efa");
    if (fabric_name[0]) h->fabric_attr->name = strdup(fabric_name);
    if (!h->fabric_attr->prov_name || (fabric_name[0] && !h->fabric_attr->name)) {
        ((free_fn)a->freeinfo)(h);
        return -FI_ENOMEM;
    }
    uint32_t api = tcp ? ((version_fn)a->version)() : FI_VERSION(1,18);
    struct fi_info *list = NULL;
    int rc = ((get_fn)a->getinfo)(api, NULL, NULL, 0, h, &list);
    ((free_fn)a->freeinfo)(h);
    *out = list;
    return rc;
}
void *orbite_fi_next(void *p) { return ((struct fi_info *)p)->next; }
void orbite_fi_freeinfo(const struct orbite_fi_api *a, void *p) {
    ((free_fn)a->freeinfo)(p);
}
int orbite_fi_describe(void *p, struct orbite_fi_info *out) {
    struct fi_info *i = p;
    if (!i || !i->domain_attr || !i->fabric_attr || !i->ep_attr || !i->tx_attr ||
        !i->domain_attr->name || !i->fabric_attr->prov_name ||
        i->ep_attr->type != FI_EP_RDM || i->domain_attr->threading != FI_THREAD_SAFE ||
        (i->domain_attr->mr_mode & FI_MR_RAW)) return -FI_ENODATA;
    uint64_t required = FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    if ((i->caps & required) != required) return -FI_ENODATA;
    uint64_t supported_mr = FI_MR_LOCAL | FI_MR_VIRT_ADDR | FI_MR_ALLOCATED |
                            FI_MR_PROV_KEY | FI_MR_ENDPOINT;
    if ((i->domain_attr->mr_mode & ~supported_mr) || !i->ep_attr->max_msg_size ||
        !(i->tx_attr->op_flags & FI_DELIVERY_COMPLETE)) return -FI_ENODATA;
    if (strlen(i->domain_attr->name) >= sizeof(out->name) ||
        strlen(i->fabric_attr->prov_name) >= sizeof(out->provider)) return -FI_EINVAL;
    strcpy(out->name, i->domain_attr->name);
    strcpy(out->provider, i->fabric_attr->prov_name);
    out->mr_mode = i->domain_attr->mr_mode;
    out->max_message = i->ep_attr->max_msg_size;
    out->tx_size = i->tx_attr->size ? i->tx_attr->size : 1024;
    out->ipv4 = i->addr_format == FI_SOCKADDR_IN;
    out->virt_addr = !!(out->mr_mode & FI_MR_VIRT_ADDR);
    out->endpoint_mr = !!(out->mr_mode & FI_MR_ENDPOINT);
    out->provider_key = !!(out->mr_mode & FI_MR_PROV_KEY);
    out->delivery_complete = !!(i->tx_attr->op_flags & FI_DELIVERY_COMPLETE);
    out->manual_progress = i->domain_attr->data_progress == FI_PROGRESS_MANUAL;
    out->numa = -1;
    if (i->nic && i->nic->bus_attr && i->nic->bus_attr->bus_type == FI_BUS_PCI) {
        struct fi_pci_attr *b = &i->nic->bus_attr->attr.pci;
        char path[128];
        snprintf(path, sizeof(path), "/sys/bus/pci/devices/%04x:%02x:%02x.%x/numa_node",
                 b->domain_id, b->bus_id, b->device_id, b->function_id);
        FILE *f = fopen(path, "r");
        if (f) { if (fscanf(f, "%d", &out->numa) != 1) out->numa = -1; fclose(f); }
    }
    return 0;
}
int orbite_fi_open(const struct orbite_fi_api *a, void *p, size_t cq_size,
                   size_t peers, int tcp, struct orbite_fi_handles *o,
                   uint8_t *address, size_t *address_len) {
    struct fi_info *i = ((dup_fn)a->dupinfo)(p);
    if (!i) return -FI_ENOMEM;
    (void)tcp;
    i->tx_attr->op_flags &= ~(FI_INJECT_COMPLETE | FI_TRANSMIT_COMPLETE);
    i->tx_attr->op_flags |= FI_DELIVERY_COMPLETE;
    if (i->tx_attr->size > cq_size) i->tx_attr->size = cq_size;
    if (i->rx_attr && i->rx_attr->size > cq_size) i->rx_attr->size = cq_size;
    int r = ((fabric_fn)a->fabric)(i->fabric_attr, (struct fid_fabric **)&o->fabric, NULL);
    if (r) goto done;
    r = fi_domain(o->fabric, i, (struct fid_domain **)&o->domain, NULL);
    if (r) goto done;
    struct fi_av_attr av = {0}; av.type = FI_AV_TABLE; av.count = peers;
    r = fi_av_open(o->domain, &av, (struct fid_av **)&o->av, NULL);
    if (r) goto done;
    struct fi_cq_attr cq = {0}; cq.format = FI_CQ_FORMAT_CONTEXT;
    cq.size = cq_size; cq.wait_obj = FI_WAIT_NONE;
    r = fi_cq_open(o->domain, &cq, (struct fid_cq **)&o->cq, NULL);
    if (r) goto done;
    r = fi_endpoint(o->domain, i, (struct fid_ep **)&o->ep, NULL);
    if (r) goto done;
    r = fi_ep_bind(o->ep, o->av, 0);
    if (r) goto done;
    r = fi_ep_bind(o->ep, o->cq, FI_TRANSMIT | FI_RECV);
    if (r) goto done;
    r = fi_enable(o->ep);
    if (r) goto done;
    r = fi_getname(o->ep, address, address_len);
done:
    ((free_fn)a->freeinfo)(i);
    /* Rust owns every nonnull output, including partial construction. */
    return r;
}
int orbite_fi_close(void *p) { return p ? fi_close(p) : 0; }
int orbite_fi_register(const struct orbite_fi_handles *h, void *p, size_t len,
                       int remote, uint64_t key, int endpoint,
                       void **out, void **desc, uint64_t *actual) {
    uint64_t access = FI_READ | FI_WRITE;
    if (remote == 1) access |= FI_REMOTE_READ;
    if (remote == 2) access |= FI_REMOTE_WRITE;
    struct fid_mr *mr = NULL;
    int r = fi_mr_reg(h->domain, p, len, access, 0, key, 0, &mr, NULL);
    *out = mr;
    if (r) return r;
    if (endpoint) {
        r = fi_mr_bind(mr, h->ep, 0);
        if (!r) r = fi_mr_enable(mr);
        if (r) return r;
    }
    *desc = fi_mr_desc(mr); *actual = fi_mr_key(mr);
    return 0;
}
int orbite_fi_insert(const struct orbite_fi_handles *h, const uint8_t *p, uint64_t *out) {
    int r = fi_av_insert(h->av, p, 1, out, 0, NULL);
    return r == 1 ? 0 : (r < 0 ? r : -FI_EINVAL);
}
int orbite_fi_remove(const struct orbite_fi_handles *h, uint64_t addr) {
    return fi_av_remove(h->av, &addr, 1, 0);
}
int orbite_fi_post(const struct orbite_fi_handles *h, int write, void *p, size_t len,
                   void *desc, uint64_t peer, uint64_t remote, uint64_t key, void *cookie) {
    return write ? (int)fi_write(h->ep, p, len, desc, peer, remote, key, cookie)
                 : (int)fi_read(h->ep, p, len, desc, peer, remote, key, cookie);
}
int orbite_fi_poll(const struct orbite_fi_handles *h, struct orbite_fi_completion *out) {
    struct fi_cq_entry entry = {0};
    ssize_t r = fi_cq_read(h->cq, &entry, 1);
    if (r == -FI_EAGAIN || r == 0) return 1;
    if (r == -FI_EAVAIL) {
        struct fi_cq_err_entry err = {0};
        r = fi_cq_readerr(h->cq, &err, 0);
        if (r < 0) return (int)r;
        if (r != 1) return -FI_EIO;
        out->context = err.op_context; out->error = err.err;
        out->provider_error = err.prov_errno; out->length = err.len;
        return 0;
    }
    if (r != 1) return r < 0 ? (int)r : -FI_EIO;
    out->context = entry.op_context; out->error = 0;
    out->provider_error = 0; out->length = 0;
    return 0;
}
