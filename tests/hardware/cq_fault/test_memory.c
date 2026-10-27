/* CPU controls for fault scope and receipt identity; never hardware evidence. */
#include "cq_fault.c"
#include <assert.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/wait.h>

static int blocked_fd, other_fd, native_calls, probes, passthrough;
static int fake_poll(struct ibv_cq *cq, int n, struct ibv_wc *wc) {
    (void)cq; (void)n; (void)wc; return 0;
}
static int fake_post(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad) {
    (void)qp; (void)bad;
    if (wr->opcode == IBV_WR_BIND_MW) wr->bind_mw.mw->rkey = wr->bind_mw.rkey;
    return 0;
}
static int filtered(void) {
    ++native_calls;
    assert(write(other_fd, "x", 1) == 1);
    assert(ioctl(blocked_fd, 0, NULL) == -1 && errno == EIO);
    assert(write(blocked_fd, "x", 1) == -1 && errno == EIO);
    return EIO;
}
static int fake_dereg(struct ibv_mr *mr) {
    if ((uintptr_t)mr->addr != 1024) { ++passthrough; return 0; }
    return filtered();
}
static int fake_dealloc(struct ibv_mw *mw) {
    (void)mw; return filtered();
}
static int fake_pd(struct ibv_pd *pd) { (void)pd; ++probes; return EBUSY; }

static void run(unsigned kind, unsigned source) {
    int blocked[2], other[2];
    assert(pipe(blocked) == 0 && pipe(other) == 0);
    blocked_fd = blocked[1]; other_fd = other[1];
    struct ibv_device device = {0};
    strcpy(device.name, "test-rail");
    struct ibv_context context[2] = {0};
    for (int i = 0; i < 2; ++i) {
        context[i].ops.poll_cq = fake_poll; context[i].ops.post_send = fake_post;
        context[i].ops.dealloc_mw = fake_dealloc;
        context[i].cmd_fd = blocked_fd;
        attach(&context[i]);
    }
    context[0].device = &device;
    struct ibv_device sibling = {0};
    strcpy(sibling.name, "sibling"); context[1].device = &sibling;
    struct ibv_pd pd = {.context = &context[0]};
    struct ibv_mr mr = {.context = &context[0], .pd = &pd, .addr = (void *)1024, .length = 65536, .lkey = 7, .rkey = 8};
    struct ibv_mw mw = {.context = &context[0], .pd = &pd, .rkey = 9};
    contexts[0].bound_mr = &mr; contexts[0].data_lkey = mr.lkey;
    native_dereg_mr = fake_dereg; native_dealloc_pd = fake_pd;
    struct memory_snapshot output;
    assert(sizeof(output) == 88);
    assert(orbitte_memory_fault(2, 0, 0, 0, NULL, &output) == EINVAL);
    assert(orbitte_memory_fault(1, kind, source, 1024, "test-rail", &output) == 0);
    assert(output.armed == 1 && output.attempts == 0);
    assert(orbitte_memory_fault(1, kind, source, 1024, "test-rail", &output) == EINVAL);
    struct ibv_mr unrelated = mr; unrelated.addr = (void *)2048;
    assert(ibv_dereg_mr(&unrelated) == 0 && passthrough == 1);
    if (kind == 1) assert(ibv_dereg_mr(&mr) == EIO);
    else {
        struct ibv_qp qp = {.context = &context[0], .qp_num = 3};
        struct ibv_send_wr wr = {.wr_id = 4, .opcode = IBV_WR_BIND_MW};
        wr.bind_mw.mw = &mw; wr.bind_mw.rkey = 10; wr.bind_mw.bind_info.mr = &mr;
        struct ibv_send_wr *bad = NULL;
        assert(post(&qp, &wr, &bad) == 0);
        assert(context[0].ops.dealloc_mw(&mw) == EIO);
    }
    assert(native_calls == 1 && write(blocked_fd, "x", 1) == 1);
    assert(orbitte_memory_fault(2, 0, 0, 0, NULL, &output) == 0);
    assert(output.mr == (uintptr_t)&mr && output.code == EIO && output.attempts == 1 && probes == 1);
    pid_t retry = fork(); assert(retry >= 0);
    if (!retry) { if (kind == 1) ibv_dereg_mr(&mr); else memory_dealloc_mw(&mw); _exit(0); }
    int status; assert(waitpid(retry, &status, 0) == retry && WIFEXITED(status) && WEXITSTATUS(status) == 125);
    for (int i = 0; i < 2; ++i) { close(blocked[i]); close(other[i]); }
}
int main(void) {
    const char *root = getenv("ORBITTE_RESTART_ROOT"); assert(root);
    for (unsigned index = 0; index < 3; ++index) {
        pid_t child = fork(); assert(child >= 0);
        if (!child) {
            char path[4096]; assert(snprintf(path, sizeof(path), "%s/%u", root, index) < (int)sizeof(path));
            assert(mkdir(path, 0700) == 0 && setenv("ORBITTE_RESTART_ROOT", path, 1) == 0);
            run(index == 2 ? 2 : 1, index != 0); _exit(0);
        }
        int status; assert(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    puts("memory fault controls passed: exact owner, native callback once, thread/FD scope, retry rejection");
    return 0;
}
