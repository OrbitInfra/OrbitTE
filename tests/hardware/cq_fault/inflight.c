/* Included by cq_fault.c: interrupt one accepted payload on its own QP.
 * The caller's connection lock keeps the QP alive through post/modify. */
struct inflight_snapshot {
    uint64_t version, armed, posts, modifications, failed, completed, errors;
    uint64_t qpn, wr_id, destroyed, sequence;
};
static struct inflight_snapshot inflight = {.version = 1};
static struct ibv_cq *inflight_cq;
static unsigned inflight_operation;
static int (*native_modify_qp)(struct ibv_qp *, struct ibv_qp_attr *, int);

/* The observer lock covers identity publication, not DMA ownership. */
static int inflight_post(struct context *context, struct ibv_qp *qp,
                         struct ibv_send_wr *wr, struct ibv_send_wr **bad) {
    if (inflight.posts || wr->next || wr->num_sge != 1 || operation(wr->opcode) != inflight_operation) fail();
    inflight.qpn = qp->qp_num; inflight.wr_id = wr->wr_id; inflight_cq = qp->send_cq;
    int code = context->post(qp, wr, bad);
    ++inflight.posts;
    if (code) ++inflight.errors;
    int modified = 0;
    if (!code) {
        struct ibv_qp_attr attr = {.qp_state = IBV_QPS_ERR};
        modified = native_modify_qp(qp, &attr, IBV_QP_STATE);
        ++inflight.modifications;
        if (modified) ++inflight.errors;
    }
    /* Keep receipt I/O outside the post-to-ERR interval. */
    unsigned index = find(qp->context);
    receipt("\"kind\":\"dma_post\",\"context\":%u,\"cq\":%" PRIuPTR ",\"qpn\":%u,\"wr_id\":%" PRIu64 ",\"operation\":%u,\"code\":%d,\"length\":%u,\"local\":%" PRIu64 ",\"remote\":%" PRIu64,
            index, (uintptr_t)qp->send_cq, qp->qp_num, wr->wr_id, inflight_operation,
            code, wr->sg_list[0].length, wr->sg_list[0].addr, wr->wr.rdma.remote_addr);
    if (!code) {
        receipt("\"kind\":\"dma_error\",\"context\":%u,\"cq\":%" PRIuPTR ",\"qpn\":%u,\"wr_id\":%" PRIu64 ",\"code\":%d,\"state\":%u",
                index, (uintptr_t)qp->send_cq, qp->qp_num, wr->wr_id, modified, IBV_QPS_ERR);
    }
    return code;
}

static void inflight_completion(struct ibv_cq *cq, const struct ibv_wc *wc) {
    if (!inflight.armed || cq != inflight_cq || wc->qp_num != inflight.qpn || wc->wr_id != inflight.wr_id) return;
    /* opcode/byte_len are not valid for an error WC. Join by QPN and WR ID. */
    if (wc->status == IBV_WC_SUCCESS) ++inflight.completed;
    else ++inflight.failed;
    if (inflight.completed + inflight.failed != 1) ++inflight.errors;
    receipt("\"kind\":\"dma_completion\",\"context\":%u,\"cq\":%" PRIuPTR ",\"qpn\":%u,\"wr_id\":%" PRIu64 ",\"status\":%u,\"vendor_err\":%u",
            find(cq->context), (uintptr_t)cq, wc->qp_num, wc->wr_id, wc->status, wc->vendor_err);
}

static void inflight_destroyed(struct ibv_cq *cq, unsigned qpn, int code) {
    if (inflight.armed && cq == inflight_cq && qpn == inflight.qpn) {
        if (code || ++inflight.destroyed != 1) ++inflight.errors;
    }
}

int orbitte_inflight_fault(unsigned command, unsigned op, struct inflight_snapshot *output) {
    if (!output) return EINVAL;
    pthread_mutex_lock(&lock);
    int error = 0;
    if (command == 1) {
        if (inflight.armed || inflight.posts || state.armed || cleanup.armed || state.errors
                || state.contexts != 2 || !(op == 1 || op == 2)) error = EINVAL;
        else {
            if (!native_modify_qp) *(void **)(&native_modify_qp) = dlsym(RTLD_NEXT, "ibv_modify_qp");
            if (!native_modify_qp) fail();
            inflight.armed = 1; inflight_operation = op;
            receipt("\"kind\":\"dma_arm\",\"operation\":%u", op);
        }
    } else if (command == 2) {
        if (!inflight.armed || inflight.posts != 1 || inflight.modifications != 1
                || inflight.failed != 1 || inflight.completed || inflight.destroyed != 1
                || inflight.errors || state.errors) error = EINVAL;
        else { inflight.armed = 0; receipt("\"kind\":\"dma_disarm\""); }
    } else if (command != 0) error = EINVAL;
    inflight.sequence = state.sequence;
    *output = inflight;
    pthread_mutex_unlock(&lock);
    return error;
}
