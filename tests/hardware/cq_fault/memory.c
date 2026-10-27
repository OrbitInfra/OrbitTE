/* Included by cq_fault.c. Witnesses never own or free the retained allocation. */
struct memory_snapshot {
    uint64_t version, armed, kind, attempts, code, mr, mw, pd, lkey, rkey, sequence;
};
static struct memory_snapshot memory_fault = {.version = 1};
static struct ibv_context *memory_context;
static struct ibv_mr *memory_mr;
static struct ibv_mw *memory_mw;
static uint64_t memory_base;
static int (*native_dereg_mr)(struct ibv_mr *);
static int (*native_dealloc_pd)(struct ibv_pd *);

struct memory_destruction { struct ibv_mr *mr; struct ibv_mw *mw; int code; };
static void *deny_memory_destroy(void *argument) {
    struct memory_destruction *call = argument;
    struct ibv_context *context = call->mw ? call->mw->context : call->mr->context;
    deny_commands(context->cmd_fd);
    call->code = call->mw ? contexts[find(context)].dealloc_mw(call->mw) : native_dereg_mr(call->mr);
    return NULL;
}

/* Caller holds the observer lock; the joined helper only calls the saved provider. */
static int memory_destroy(struct ibv_mr *mr, struct ibv_mw *mw) {
    if (++memory_fault.attempts != 1) fail();
    memory_mr = mr; memory_mw = mw;
    memory_fault.mr = (uintptr_t)mr; memory_fault.mw = (uintptr_t)mw;
    memory_fault.pd = (uintptr_t)mr->pd;
    memory_fault.lkey = mr->lkey; memory_fault.rkey = mw ? mw->rkey : mr->rkey;
    struct memory_destruction call = {mr, mw, 0};
    pthread_t thread;
    if (pthread_create(&thread, NULL, deny_memory_destroy, &call) || pthread_join(thread, NULL)) fail();
    memory_fault.code = call.code;
    receipt("\"kind\":\"memory_error\",\"resource\":%" PRIu64 ",\"context\":%u,\"mr\":%" PRIu64 ",\"mw\":%" PRIu64 ",\"pd\":%" PRIu64 ",\"lkey\":%" PRIu64 ",\"rkey\":%" PRIu64 ",\"address\":%" PRIu64 ",\"length\":%zu,\"code\":%d,\"attempts\":1,\"syscall_fault\":true",
            memory_fault.kind, find(mr->context), memory_fault.mr, memory_fault.mw, memory_fault.pd,
            memory_fault.lkey, memory_fault.rkey, (uint64_t)(uintptr_t)mr->addr, mr->length, call.code);
    if (call.code != EIO) fail();
    return call.code;
}

int ibv_dereg_mr(struct ibv_mr *mr) {
    pthread_mutex_lock(&lock);
    if (!native_dereg_mr) *(void **)(&native_dereg_mr) = dlsym(RTLD_NEXT, "ibv_dereg_mr");
    if (!native_dereg_mr) fail();
    if (memory_fault.attempts && mr == memory_mr) fail();
    int selected = memory_fault.armed && memory_fault.kind == 1 && !memory_fault.attempts
        && mr->context == memory_context && (uint64_t)(uintptr_t)mr->addr == memory_base
        && (memory_mr ? mr == memory_mr : mr->lkey == memory_fault.lkey);
    if (selected) {
        int code = memory_destroy(mr, NULL);
        pthread_mutex_unlock(&lock);
        return code;
    }
    pthread_mutex_unlock(&lock);
    return native_dereg_mr(mr);
}

static int memory_dealloc_mw(struct ibv_mw *mw) {
    pthread_mutex_lock(&lock);
    if (memory_fault.attempts && mw == memory_mw) fail();
    int (*native)(struct ibv_mw *) = contexts[find(mw->context)].dealloc_mw;
    if (memory_fault.armed && memory_fault.kind == 2 && mw == memory_mw) {
        int code = memory_destroy(memory_mr, mw);
        pthread_mutex_unlock(&lock);
        return code;
    }
    pthread_mutex_unlock(&lock);
    return native(mw);
}

static int memory_bind(struct context *context, struct ibv_qp *qp,
                       struct ibv_send_wr *wr, struct ibv_send_wr **bad) {
    struct ibv_mr *mr = wr->bind_mw.bind_info.mr;
    if (wr->next || memory_mw || !mr || (uint64_t)(uintptr_t)mr->addr != memory_base) fail();
    memory_mr = mr; memory_mw = wr->bind_mw.mw;
    int code = context->post(qp, wr, bad);
    receipt("\"kind\":\"memory_bind\",\"context\":%u,\"mr\":%" PRIuPTR ",\"mw\":%" PRIuPTR ",\"qpn\":%u,\"wr_id\":%" PRIu64 ",\"key\":%u,\"code\":%d",
            find(qp->context), (uintptr_t)mr, (uintptr_t)memory_mw, qp->qp_num, wr->wr_id, wr->bind_mw.rkey, code);
    if (code) fail();
    return code;
}

int orbitte_memory_fault(unsigned command, unsigned kind, unsigned source, uint64_t base,
                         const char *rail, struct memory_snapshot *output) {
    if (!output) return EINVAL;
    pthread_mutex_lock(&lock);
    int error = 0;
    if (command == 1) {
        if (memory_fault.armed || state.armed || inflight.armed || cleanup.armed || state.errors
                || state.contexts != 2 || !(kind == 1 || kind == 2) || source > 1
                || (kind == 2 && !source) || !rail || !base) error = EINVAL;
        else {
            for (unsigned i = 0; i < 8; ++i)
                if (contexts[i].owner && !strcmp(contexts[i].owner->device->name, rail)) {
                    if (memory_context) fail();
                    memory_context = contexts[i].owner;
                    memory_mr = source ? contexts[i].bound_mr : NULL;
                    memory_fault.lkey = source ? (memory_mr ? memory_mr->lkey : 0) : contexts[i].data_lkey;
                }
            if (!memory_context || !memory_fault.lkey || (source && !memory_mr)) error = EINVAL;
            else {
                memory_fault.armed = 1; memory_fault.kind = kind; memory_base = base;
                receipt("\"kind\":\"memory_arm\",\"resource\":%u,\"source\":%u,\"context\":%u,\"address\":%" PRIu64, kind, source, find(memory_context), base);
            }
        }
    } else if (command == 2) {
        if (memory_fault.attempts != 1 || memory_fault.code != EIO || !memory_mr
                || (uintptr_t)memory_mr->pd != memory_fault.pd || memory_mr->lkey != memory_fault.lkey
                || (memory_mw ? memory_mw->rkey : memory_mr->rkey) != memory_fault.rkey) error = EINVAL;
        else {
            if (!native_dealloc_pd) *(void **)(&native_dealloc_pd) = dlsym(RTLD_NEXT, "ibv_dealloc_pd");
            if (!native_dealloc_pd) fail();
            /* Not a query-MR claim: the real PD destructor must still see native children.
             * Unexpected success terminates the process before any owner can use that PD. */
            int code = native_dealloc_pd(memory_mr->pd);
            if (code != EBUSY) fail();
            receipt("\"kind\":\"memory_retained\",\"resource\":%" PRIu64 ",\"context\":%u,\"mr\":%" PRIu64 ",\"mw\":%" PRIu64 ",\"pd\":%" PRIu64 ",\"code\":%d,\"keys_intact\":true",
                    memory_fault.kind, find(memory_context), memory_fault.mr, memory_fault.mw, memory_fault.pd, code);
        }
    } else if (command != 0) error = EINVAL;
    memory_fault.sequence = state.sequence;
    *output = memory_fault;
    pthread_mutex_unlock(&lock);
    return error;
}
