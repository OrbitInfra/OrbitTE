/* Bounded pass-through ledger; no completion is delayed, removed or fabricated. */
struct accounting_snapshot {
    uint64_t version, active, posts, completed, bytes, pending, peak, rail_peak[2], sequence;
};
struct accounting_entry {
    struct ibv_cq *cq;
    uint64_t id;
    uint32_t qpn, length;
    unsigned context, completed;
};
static struct accounting_snapshot accounting = {.version = 1};
static struct accounting_entry accounting_entries[256];
static unsigned accounting_operation, accounting_limit, accounting_errors;
static uint64_t accounting_rails[2];

/* Caller keeps the observer lock across the real post and its receipt, so a
 * concurrently polled completion cannot appear before its accepted WR. */
static int accounting_post(const struct context *context, unsigned index,
                           struct ibv_qp *qp, struct ibv_send_wr *wr,
                           struct ibv_send_wr **bad) {
    int code = context->post(qp, wr, bad);
    for (struct ibv_send_wr *entry = wr; entry; entry = entry->next) {
        unsigned op = operation(entry->opcode);
        if (!op) continue;
        if (code || index >= 2 || op != accounting_operation || entry->num_sge != 1
                || accounting.posts >= accounting_limit) { ++accounting_errors; continue; }
        for (unsigned i = 0; i < accounting.posts; ++i)
            if (accounting_entries[i].cq == qp->send_cq && accounting_entries[i].qpn == qp->qp_num
                    && accounting_entries[i].id == entry->wr_id) ++accounting_errors;
        accounting_entries[accounting.posts++] = (struct accounting_entry){
            .cq = qp->send_cq, .qpn = qp->qp_num, .id = entry->wr_id,
            .length = entry->sg_list[0].length, .context = index};
        if (++accounting.pending > accounting.peak) accounting.peak = accounting.pending;
        if (++accounting_rails[index] > accounting.rail_peak[index])
            accounting.rail_peak[index] = accounting_rails[index];
        receipt("\"kind\":\"rail_post\",\"context\":%u,\"cq\":%" PRIuPTR ",\"qpn\":%u,\"wr_id\":%" PRIu64 ",\"operation\":%u,\"code\":%d,\"length\":%u,\"local\":%" PRIu64 ",\"remote\":%" PRIu64,
                index, (uintptr_t)qp->send_cq, qp->qp_num, entry->wr_id, op, code,
                entry->sg_list[0].length, entry->sg_list[0].addr, entry->wr.rdma.remote_addr);
    }
    return code;
}

static void accounting_complete(struct ibv_cq *cq, const struct ibv_wc *wc) {
    if (!accounting.active) return;
    struct accounting_entry *entry = NULL;
    for (unsigned i = 0; i < accounting.posts; ++i)
        if (accounting_entries[i].cq == cq && accounting_entries[i].qpn == wc->qp_num
                && accounting_entries[i].id == wc->wr_id) { entry = &accounting_entries[i]; break; }
    unsigned op = wc->status == IBV_WC_SUCCESS
        ? (wc->opcode == IBV_WC_RDMA_READ ? 1 : wc->opcode == IBV_WC_RDMA_WRITE ? 2 : 0) : 0;
    if (!entry) { if (op) ++accounting_errors; return; }
    if (entry->completed || wc->status != IBV_WC_SUCCESS || op != accounting_operation
            || (op == 1 && wc->byte_len != entry->length)) { ++accounting_errors; return; }
    entry->completed = 1;
    ++accounting.completed;
    accounting.bytes += entry->length;
    --accounting.pending;
    --accounting_rails[entry->context];
    receipt("\"kind\":\"rail_complete\",\"context\":%u,\"cq\":%" PRIuPTR ",\"qpn\":%u,\"wr_id\":%" PRIu64 ",\"operation\":%u,\"status\":%u,\"length\":%u",
            entry->context, (uintptr_t)cq, wc->qp_num, wc->wr_id, op, wc->status, wc->byte_len);
}

int orbitte_rail_accounting(unsigned command, unsigned phase, unsigned op,
                           unsigned limit, struct accounting_snapshot *output) {
    if (!output) return EINVAL;
    pthread_mutex_lock(&lock);
    int error = 0;
    if (command == 1) {
        if (accounting.active || accounting_errors || state.armed || inflight.armed
                || memory_fault.armed || dependency.armed || cleanup.armed || state.contexts != 2
                || !(op == 1 || op == 2) || !limit || limit > 256) error = EINVAL;
        else {
            accounting = (struct accounting_snapshot){.version = 1, .active = 1};
            memset(accounting_entries, 0, sizeof(accounting_entries));
            memset(accounting_rails, 0, sizeof(accounting_rails));
            accounting_operation = op; accounting_limit = limit; state.phase = phase;
            receipt("\"kind\":\"rail_arm\",\"operation\":%u,\"expected\":%u", op, limit);
        }
    } else if (command == 2) {
        if (!accounting.active || accounting_errors || accounting.pending
                || accounting.posts != accounting_limit || accounting.completed != accounting_limit) error = EINVAL;
        else {
            accounting.active = 0;
            receipt("\"kind\":\"rail_done\",\"posts\":%" PRIu64 ",\"completed\":%" PRIu64 ",\"bytes\":%" PRIu64, accounting.posts, accounting.completed, accounting.bytes);
        }
    } else if (command != 0 || accounting_errors) error = EINVAL;
    accounting.sequence = state.sequence;
    *output = accounting;
    pthread_mutex_unlock(&lock);
    return error;
}
