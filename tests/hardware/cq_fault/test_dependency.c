/* CPU controls verify observer scope and dependency identity, not RDMA behavior. */
#include "cq_fault.c"
#include <assert.h>
#include <sys/stat.h>
#include <sys/wait.h>

static struct ibv_qp blocker_qp;
static struct ibv_cq blocker_cq;
static struct ibv_mw blocker_mw;
static int destructor_calls, probe_calls, unrelated_calls;
static unsigned test_kind;
static void resolved(void) {}
static int fake_poll(struct ibv_cq *cq, int n, struct ibv_wc *wc) {
    assert(cq == &blocker_cq && n == 1); (void)wc; ++probe_calls; return 0;
}
static int fake_post(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad) {
    (void)qp; (void)wr; (void)bad; return 0;
}
static int fake_close(struct ibv_context *context) { (void)context; assert(0); return 0; }
static struct ibv_qp *fake_create_qp(struct ibv_pd *pd, struct ibv_qp_init_attr *init) {
    assert(init->send_cq == init->recv_cq && init->qp_type == IBV_QPT_RC);
    blocker_qp = (struct ibv_qp){.context = pd->context, .pd = pd, .send_cq = init->send_cq,
        .recv_cq = init->recv_cq, .qp_num = 99};
    return &blocker_qp;
}
static struct ibv_cq *fake_create_cq(struct ibv_context *context, int size, void *user,
                                   struct ibv_comp_channel *channel, int vector) {
    assert(size == 1 && !user && vector == 0 && channel->context == context);
    blocker_cq = (struct ibv_cq){.context = context, .channel = channel};
    ++channel->refcnt;
    return &blocker_cq;
}
static struct ibv_mw *fake_alloc_mw(struct ibv_pd *pd, enum ibv_mw_type type) {
    assert(type == IBV_MW_TYPE_2);
    blocker_mw = (struct ibv_mw){.context = pd->context, .pd = pd, .rkey = 123};
    return &blocker_mw;
}
static int fake_destroy_cq(struct ibv_cq *cq) {
    if ((uintptr_t)cq != dependency.cq) { ++unrelated_calls; return 0; }
    ++destructor_calls;
    assert(blocker_qp.send_cq == cq && blocker_qp.recv_cq == cq); return EBUSY;
}
static int fake_destroy_channel(struct ibv_comp_channel *channel) {
    ++destructor_calls;
    assert(blocker_cq.channel == channel && channel->refcnt > 0); return EBUSY;
}
static int fake_dealloc_pd(struct ibv_pd *pd) {
    if (!dependency.attempts) { ++unrelated_calls; return 0; }
    ++destructor_calls;
    assert(blocker_mw.pd == pd); return EBUSY;
}
static int fake_query(struct ibv_qp *qp, struct ibv_qp_attr *attr, int mask, struct ibv_qp_init_attr *init) {
    (void)init; assert(qp == &blocker_qp && mask == IBV_QP_STATE);
    ++probe_calls; attr->qp_state = IBV_QPS_RESET; return 0;
}
static int destroy(struct ibv_cq *cq, struct ibv_comp_channel *channel, struct ibv_pd *pd) {
    return test_kind == 1 ? ibv_destroy_cq(cq) : test_kind == 2 ? ibv_destroy_comp_channel(channel) : ibv_dealloc_pd(pd);
}
static void run(unsigned kind) {
    test_kind = kind;
    native_create_qp = fake_create_qp; native_create_cq = fake_create_cq;
    native_destroy_cq = fake_destroy_cq; native_destroy_channel = fake_destroy_channel;
    native_dealloc_pd = fake_dealloc_pd; native_query_qp = fake_query; native_close = fake_close;
    pthread_once(&symbols, resolved);
    struct ibv_device device[2] = {0};
    strcpy(device[0].name, "selected"); strcpy(device[1].name, "sibling");
    struct ibv_context context[2] = {0};
    for (int i = 0; i < 2; ++i) {
        context[i].device = &device[i]; context[i].ops.poll_cq = fake_poll;
        context[i].ops.post_send = fake_post; context[i].ops.alloc_mw = fake_alloc_mw;
        attach(&context[i]);
    }
    int pipefd[2]; assert(pipe(pipefd) == 0);
    struct ibv_pd pd = {.context = &context[0]};
    struct ibv_comp_channel channel = {.context = &context[0], .fd = pipefd[0]};
    struct ibv_cq cq = {.context = &context[0], .channel = &channel};
    struct ibv_qp qp = {.context = &context[0], .pd = &pd, .send_cq = &cq, .qp_num = 7};
    contexts[0].last_qp = &qp;
    state.armed = kind != 3; state.posts = state.lost = expected = 1;
    struct dependency_snapshot output;
    assert(sizeof(output) == 104);
    assert(orbitte_dependency_fault(2, 0, NULL, &output) == EINVAL);
    assert(orbitte_dependency_fault(1, kind, "sibling", &output) == EINVAL);
    assert(orbitte_dependency_fault(1, kind, "selected", &output) == 0);
    assert(output.armed == 1 && output.kind == kind && output.attempts == 0 && output.qpn == 7);
    assert(orbitte_dependency_fault(1, kind, "selected", &output) == EINVAL);
    struct ibv_cq unrelated = {.context = &context[1]};
    assert(ibv_destroy_cq(&unrelated) == 0 && unrelated_calls == 1);
    assert(destroy(&cq, &channel, &pd) == EBUSY && destructor_calls == 1);
    for (int i = 0; i < 2; ++i) assert(orbitte_dependency_fault(2, 0, NULL, &output) == 0);
    assert(output.live == 1 && output.attempts == 1 && output.code == EBUSY && output.blocker != 0);
    assert(kind == 3 ? destructor_calls == 3 : probe_calls == 2);
    for (int action = 0; action < 2; ++action) {
        pid_t child = fork(); assert(child >= 0);
        if (!child) { if (action) ibv_close_device(&context[0]); else destroy(&cq, &channel, &pd); _exit(0); }
        int status; assert(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 125);
    }
    close(pipefd[0]); close(pipefd[1]);
}
int main(void) {
    const char *root = getenv("ORBITTE_RESTART_ROOT"); assert(root);
    for (unsigned kind = 1; kind <= 3; ++kind) {
        pid_t child = fork(); assert(child >= 0);
        if (!child) {
            char path[4096]; assert(snprintf(path, sizeof(path), "%s/%u", root, kind) < (int)sizeof(path));
            assert(mkdir(path, 0700) == 0 && setenv("ORBITTE_RESTART_ROOT", path, 1) == 0);
            run(kind); _exit(0);
        }
        int status; assert(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    puts("dependency controls passed: selected real-child contract, native call, post-drop probe, retry/parent-close rejection");
}
