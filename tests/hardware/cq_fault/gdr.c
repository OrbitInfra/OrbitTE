/* Passive GPU registration/WR witnesses. No pointer ownership or data copying. */
static int gdr_observe(void) {
    const char *value = getenv("ORBITTE_GDR_OBSERVE");
    return value && !strcmp(value, "1");
}

static void gdr_registration(struct ibv_pd *pd, void *address, size_t length,
                             unsigned access, struct ibv_mr *mr) {
    if (!gdr_observe()) return;
    void *driver = dlopen("libcuda.so.1", RTLD_NOW | RTLD_NOLOAD);
    int (*attribute)(void *, int, uint64_t) = NULL;
    if (driver) *(void **)(&attribute) = dlsym(driver, "cuPointerGetAttribute");
    unsigned memory_type = 0;
    int status = attribute ? attribute(&memory_type, 2, (uint64_t)(uintptr_t)address) : -1;
    if (driver) dlclose(driver);
    pthread_mutex_lock(&lock);
    receipt("\"kind\":\"gdr_mr\",\"context\":%u,\"pd\":%" PRIuPTR ",\"mr\":%" PRIuPTR ",\"address\":%" PRIuPTR ",\"length\":%zu,\"access\":%u,\"cuda_status\":%d,\"memory_type\":%u,\"lkey\":%u,\"rkey\":%u",
            find(pd->context), (uintptr_t)pd, (uintptr_t)mr, (uintptr_t)address,
            length, access, status, memory_type, mr ? mr->lkey : 0, mr ? mr->rkey : 0);
    pthread_mutex_unlock(&lock);
}

#undef ibv_reg_mr
struct ibv_mr *ibv_reg_mr(struct ibv_pd *pd, void *address, size_t length, int access) {
    struct ibv_mr *(*native)(struct ibv_pd *, void *, size_t, int);
    *(void **)(&native) = dlsym(RTLD_NEXT, "ibv_reg_mr");
    if (!native) fail();
    struct ibv_mr *mr = native(pd, address, length, access);
    int saved_errno = errno;
    gdr_registration(pd, address, length, (unsigned)access, mr);
    errno = saved_errno;
    return mr;
}

struct ibv_mr *ibv_reg_mr_iova2(struct ibv_pd *pd, void *address, size_t length,
                               uint64_t iova, unsigned access) {
    struct ibv_mr *(*native)(struct ibv_pd *, void *, size_t, uint64_t, unsigned);
    *(void **)(&native) = dlsym(RTLD_NEXT, "ibv_reg_mr_iova2");
    if (!native) fail();
    struct ibv_mr *mr = native(pd, address, length, iova, access);
    int saved_errno = errno;
    gdr_registration(pd, address, length, access, mr);
    errno = saved_errno;
    return mr;
}

struct ibv_qp *ibv_create_qp(struct ibv_pd *pd, struct ibv_qp_init_attr *init) {
    pthread_mutex_lock(&lock);
    dependency_symbols();
    pthread_mutex_unlock(&lock);
    struct ibv_qp *qp = native_create_qp(pd, init);
    int saved_errno = errno;
    if (gdr_observe()) {
        pthread_mutex_lock(&lock);
        receipt("\"kind\":\"gdr_qp\",\"context\":%u,\"pd\":%" PRIuPTR ",\"qpn\":%u,\"sq_sig_all\":%d", find(pd->context), (uintptr_t)pd, qp ? qp->qp_num : 0, init->sq_sig_all);
        pthread_mutex_unlock(&lock);
    }
    errno = saved_errno;
    return qp;
}

static void gdr_post(struct ibv_qp *qp, struct ibv_send_wr *wr, int code) {
    if (!gdr_observe()) return;
    pthread_mutex_lock(&lock);
    for (struct ibv_send_wr *entry = wr; entry; entry = entry->next) {
        receipt("\"kind\":\"gdr_post\",\"context\":%u,\"pd\":%" PRIuPTR ",\"qpn\":%u,\"wr_id\":%" PRIu64 ",\"opcode\":%u,\"code\":%d,\"length\":%u,\"local\":%" PRIu64 ",\"lkey\":%u,\"remote\":%" PRIu64 ",\"rkey\":%u",
                find(qp->context), (uintptr_t)qp->pd, qp->qp_num, entry->wr_id,
                entry->opcode, code, entry->num_sge == 1 ? entry->sg_list[0].length : 0,
                entry->num_sge == 1 ? entry->sg_list[0].addr : 0,
                entry->num_sge == 1 ? entry->sg_list[0].lkey : 0,
                operation(entry->opcode) ? entry->wr.rdma.remote_addr : 0,
                operation(entry->opcode) ? entry->wr.rdma.rkey : 0);
    }
    pthread_mutex_unlock(&lock);
}
