/* Real native children prevent destruction; no synthetic provider return codes. */
struct dependency_snapshot {
    uint64_t version, armed, kind, attempts, code, context, pd, cq, channel, qpn, blocker, live, sequence;
};
static struct dependency_snapshot dependency = {.version = 1};
static struct ibv_context *dependency_context;
static int (*native_destroy_cq)(struct ibv_cq *);
static int (*native_destroy_channel)(struct ibv_comp_channel *);
static struct ibv_qp *(*native_create_qp)(struct ibv_pd *, struct ibv_qp_init_attr *);
static struct ibv_cq *(*native_create_cq)(struct ibv_context *, int, void *, struct ibv_comp_channel *, int);

static void dependency_symbols(void) {
    if (!native_destroy_cq) *(void **)(&native_destroy_cq) = dlsym(RTLD_NEXT, "ibv_destroy_cq");
    if (!native_destroy_channel) *(void **)(&native_destroy_channel) = dlsym(RTLD_NEXT, "ibv_destroy_comp_channel");
    if (!native_create_qp) *(void **)(&native_create_qp) = dlsym(RTLD_NEXT, "ibv_create_qp");
    if (!native_create_cq) *(void **)(&native_create_cq) = dlsym(RTLD_NEXT, "ibv_create_cq");
    if (!native_dealloc_pd) *(void **)(&native_dealloc_pd) = dlsym(RTLD_NEXT, "ibv_dealloc_pd");
    if (!native_destroy_cq || !native_destroy_channel || !native_create_qp || !native_create_cq || !native_dealloc_pd) fail();
}

static void dependency_error(int code) {
    dependency.code = code;
    receipt("\"kind\":\"dependency_error\",\"resource\":%" PRIu64 ",\"context\":%" PRIu64 ",\"pd\":%" PRIu64 ",\"cq\":%" PRIu64 ",\"channel\":%" PRIu64 ",\"qpn\":%" PRIu64 ",\"blocker\":%" PRIu64 ",\"code\":%d,\"attempts\":%" PRIu64,
            dependency.kind, dependency.context, dependency.pd, dependency.cq, dependency.channel,
            dependency.qpn, dependency.blocker, code, dependency.attempts);
    if (code != EBUSY) fail();
}

static void dependency_release(unsigned owner, uintptr_t handle, struct ibv_context *context, int code) {
    if (dependency.armed && context == dependency_context) {
        receipt("\"kind\":\"dependency_release\",\"owner\":%u,\"handle\":%" PRIuPTR ",\"context\":%u,\"code\":%d",
                owner, handle, find(context), code);
        if (code) fail();
    }
}

int ibv_destroy_cq(struct ibv_cq *cq) {
    pthread_mutex_lock(&lock);
    dependency_symbols();
    int selected = dependency.armed && dependency.kind == 1 && (uintptr_t)cq == dependency.cq;
    int code;
    if (selected) {
        if (++dependency.attempts != 1) fail();
        struct ibv_qp_init_attr init = {.send_cq = cq, .recv_cq = cq, .qp_type = IBV_QPT_RC,
            .cap = {.max_send_wr = 1, .max_recv_wr = 1, .max_send_sge = 1, .max_recv_sge = 1}};
        struct ibv_qp *blocker = native_create_qp((struct ibv_pd *)(uintptr_t)dependency.pd, &init);
        if (!blocker) fail();
        dependency.blocker = (uintptr_t)blocker;
        code = native_destroy_cq(cq);
        dependency_error(code);
    } else {
        struct ibv_context *context = cq->context;
        uintptr_t handle = (uintptr_t)cq;
        code = native_destroy_cq(cq);
        dependency_release(1, handle, context, code);
    }
    pthread_mutex_unlock(&lock);
    return code;
}

int ibv_destroy_comp_channel(struct ibv_comp_channel *channel) {
    pthread_mutex_lock(&lock);
    dependency_symbols();
    int selected = dependency.armed && dependency.kind == 2 && (uintptr_t)channel == dependency.channel;
    int code;
    if (selected) {
        if (++dependency.attempts != 1) fail();
        struct ibv_cq *blocker = native_create_cq(channel->context, 1, NULL, channel, 0);
        if (!blocker) fail();
        dependency.blocker = (uintptr_t)blocker;
        code = native_destroy_channel(channel);
        dependency_error(code);
    } else {
        struct ibv_context *context = channel->context;
        uintptr_t handle = (uintptr_t)channel;
        code = native_destroy_channel(channel);
        dependency_release(2, handle, context, code);
    }
    pthread_mutex_unlock(&lock);
    return code;
}

int ibv_dealloc_pd(struct ibv_pd *pd) {
    pthread_mutex_lock(&lock);
    dependency_symbols();
    int selected = dependency.armed && (uintptr_t)pd == dependency.pd;
    int code;
    if (selected) {
        if (dependency.kind != 3 || ++dependency.attempts != 1) fail();
        if (!pd->context->ops.alloc_mw) fail();
        struct ibv_mw *blocker = pd->context->ops.alloc_mw(pd, IBV_MW_TYPE_2);
        if (!blocker) fail();
        dependency.blocker = (uintptr_t)blocker;
        code = native_dealloc_pd(pd);
        dependency_error(code);
    } else code = native_dealloc_pd(pd);
    pthread_mutex_unlock(&lock);
    return code;
}

/* Raw handles are witnesses, not owners of any OrbitTE allocation. Only the
 * extra native blocker is test-owned; the kernel releases it on process exit. */
int orbitte_dependency_fault(unsigned command, unsigned kind, const char *rail,
                             struct dependency_snapshot *output) {
    if (!output) return EINVAL;
    pthread_mutex_lock(&lock);
    dependency_symbols();
    int error = 0;
    if (command == 1) {
        if (dependency.armed || cleanup.armed || memory_fault.armed || inflight.armed || state.errors
                || state.contexts != 2 || kind < 1 || kind > 3 || !rail
                || (kind == 3 ? state.armed : (!state.armed || expected != 1 || state.posts != 1 || state.lost != 1))) error = EINVAL;
        else {
            struct ibv_qp *qp = NULL;
            for (unsigned i = 0; i < 8; ++i)
                if (contexts[i].owner && !strcmp(contexts[i].owner->device->name, rail)) {
                    if (qp) fail();
                    qp = contexts[i].last_qp;
                }
            if (!qp || !qp->pd || !qp->send_cq || (kind == 2 && !qp->send_cq->channel)) error = EINVAL;
            else {
                dependency_context = qp->context;
                dependency.armed = 1; dependency.kind = kind; dependency.context = find(qp->context);
                dependency.pd = (uintptr_t)qp->pd; dependency.cq = (uintptr_t)qp->send_cq;
                dependency.channel = (uintptr_t)qp->send_cq->channel; dependency.qpn = qp->qp_num;
                receipt("\"kind\":\"arm_dependency\",\"resource\":%u,\"context\":%" PRIu64 ",\"pd\":%" PRIu64 ",\"cq\":%" PRIu64 ",\"channel\":%" PRIu64 ",\"qpn\":%" PRIu64,
                        kind, dependency.context, dependency.pd, dependency.cq, dependency.channel, dependency.qpn);
            }
        }
    } else if (command == 2) {
        if (dependency.attempts != 1 || dependency.code != EBUSY || !dependency.blocker) error = EINVAL;
        else {
            int code = 0;
            if (dependency.kind == 1) {
                struct ibv_qp *qp = (struct ibv_qp *)(uintptr_t)dependency.blocker;
                struct ibv_qp_attr attr = {0}; struct ibv_qp_init_attr init = {0};
                code = native_query_qp(qp, &attr, IBV_QP_STATE, &init);
                if (code || attr.qp_state != IBV_QPS_RESET || (uintptr_t)qp->pd != dependency.pd
                        || (uintptr_t)qp->send_cq != dependency.cq) fail();
            } else if (dependency.kind == 2) {
                struct ibv_cq *cq = (struct ibv_cq *)(uintptr_t)dependency.blocker;
                struct ibv_comp_channel *channel = (struct ibv_comp_channel *)(uintptr_t)dependency.channel;
                struct ibv_wc wc;
                code = contexts[find(dependency_context)].poll(cq, 1, &wc);
                if (code || cq->channel != channel || channel->context != dependency_context
                        || channel->refcnt <= 0 || fcntl(channel->fd, F_GETFD) < 0) fail();
            } else {
                struct ibv_mw *mw = (struct ibv_mw *)(uintptr_t)dependency.blocker;
                if ((uintptr_t)mw->pd != dependency.pd || mw->context != dependency_context) fail();
                code = native_dealloc_pd(mw->pd);
                if (code != EBUSY) fail();
            }
            dependency.live = 1;
            receipt("\"kind\":\"dependency_query\",\"resource\":%" PRIu64 ",\"context\":%" PRIu64 ",\"blocker\":%" PRIu64 ",\"code\":%d,\"live\":true",
                    dependency.kind, dependency.context, dependency.blocker, code);
        }
    } else if (command != 0) error = EINVAL;
    dependency.sequence = state.sequence;
    *output = dependency;
    pthread_mutex_unlock(&lock);
    return error;
}
