/* Process-local qualification tool. Real provider calls are never fabricated. */
#define _GNU_SOURCE
#include <infiniband/verbs.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <stddef.h>
#include <sys/prctl.h>
#include <sys/syscall.h>

struct snapshot {
    uint64_t version, armed, phase, posts, lost, errors, contexts, sequence, destroyed_qps;
};
struct context {
    struct ibv_context *owner;
    int (*poll)(struct ibv_cq *, int, struct ibv_wc *);
    int (*post)(struct ibv_qp *, struct ibv_send_wr *, struct ibv_send_wr **);
};
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static struct context contexts[8];
static struct snapshot state = {.version = 1};
static unsigned selected_operation, expected;
static int log_fd = -1;
static struct ibv_context *(*native_open)(struct ibv_device *);
static int (*native_close)(struct ibv_context *);
static int (*native_destroy_qp)(struct ibv_qp *);
struct cleanup_snapshot { uint64_t version, armed, attempts, code, qpn, live, sequence; };
static struct cleanup_snapshot cleanup = {.version = 1};
static struct ibv_qp *payload_qp;
static int (*native_query_qp)(struct ibv_qp *, struct ibv_qp_attr *, int, struct ibv_qp_init_attr *);
static pthread_once_t symbols = PTHREAD_ONCE_INIT;

static void fail(void) { _exit(125); }

/* Caller holds lock. One append is one complete receipt, separate from stdout. */
static void receipt(const char *format, ...) {
    if (log_fd < 0 || state.sequence >= 4096) fail();
    char message[1024], line[1200];
    va_list args;
    va_start(args, format);
    int length = vsnprintf(message, sizeof(message), format, args);
    va_end(args);
    if (length < 0 || (size_t)length >= sizeof(message)) fail();
    int size = snprintf(line, sizeof(line), "{\"seq\":%" PRIu64 ",\"pid\":%ld,\"phase\":%" PRIu64 ",%s}\n",
                        ++state.sequence, (long)getpid(), state.phase, message);
    if (size < 0 || (size_t)size >= sizeof(line) || write(log_fd, line, (size_t)size) != size) fail();
}

static void resolve(void) {
    *(void **)(&native_open) = dlsym(RTLD_NEXT, "ibv_open_device");
    *(void **)(&native_close) = dlsym(RTLD_NEXT, "ibv_close_device");
    *(void **)(&native_destroy_qp) = dlsym(RTLD_NEXT, "ibv_destroy_qp");
    *(void **)(&native_query_qp) = dlsym(RTLD_NEXT, "ibv_query_qp");
    if (!native_open || !native_close || !native_destroy_qp || !native_query_qp) fail();
}

static unsigned find(struct ibv_context *context) {
    for (unsigned index = 0; index < 8; ++index)
        if (contexts[index].owner == context) return index;
    fail();
    return 0;
}

static unsigned operation(enum ibv_wr_opcode op) {
    return op == IBV_WR_RDMA_READ ? 1 : op == IBV_WR_RDMA_WRITE ? 2 : 0;
}

#include "inflight.c"

static int post(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad) {
    pthread_mutex_lock(&lock);
    unsigned index = find(qp->context);
    struct context context = contexts[index];
    if (inflight.armed && operation(wr->opcode)) {
        int code = inflight_post(&context, qp, wr, bad);
        pthread_mutex_unlock(&lock);
        return code;
    }
    uint64_t phase = state.phase;
    int armed = state.armed;
    pthread_mutex_unlock(&lock);
    int code = context.post(qp, wr, bad);
    if (!armed) return code;
    pthread_mutex_lock(&lock);
    if (!state.armed || phase != state.phase) fail();
    for (struct ibv_send_wr *entry = wr; entry; entry = entry->next) {
        unsigned op = operation(entry->opcode);
        if (!op) continue;
        if (code || op != selected_operation || entry->num_sge != 1 || state.posts >= expected) {
            ++state.errors;
        } else {
            ++state.posts;
            if (expected == 1) payload_qp = qp;
        }
        receipt("\"kind\":\"post\",\"context\":%u,\"cq\":%" PRIuPTR ",\"qpn\":%u,\"wr_id\":%" PRIu64 ",\"operation\":%u,\"code\":%d,\"length\":%u,\"local\":%" PRIu64 ",\"remote\":%" PRIu64,
                index, (uintptr_t)qp->send_cq, qp->qp_num, entry->wr_id, op, code,
                entry->num_sge == 1 ? entry->sg_list[0].length : 0,
                entry->num_sge == 1 ? entry->sg_list[0].addr : 0, entry->wr.rdma.remote_addr);
    }
    pthread_mutex_unlock(&lock);
    return code;
}

static int poll(struct ibv_cq *cq, int capacity, struct ibv_wc *wc) {
    pthread_mutex_lock(&lock);
    unsigned index = find(cq->context);
    struct context context = contexts[index];
    pthread_mutex_unlock(&lock);
    int count = context.poll(cq, capacity, wc);
    if (count <= 0) return count;
    if (count > capacity) fail();
    pthread_mutex_lock(&lock);
    int kept = 0;
    for (int i = 0; i < count; ++i) {
        inflight_completion(cq, &wc[i]);
        unsigned op = 0;
        if (wc[i].status == IBV_WC_SUCCESS)
            op = wc[i].opcode == IBV_WC_RDMA_READ ? 1 : wc[i].opcode == IBV_WC_RDMA_WRITE ? 2 : 0;
        if (state.armed && wc[i].status == IBV_WC_SUCCESS && op == selected_operation) {
            if (state.lost >= expected) ++state.errors;
            ++state.lost;
            receipt("\"kind\":\"lost\",\"context\":%u,\"cq\":%" PRIuPTR ",\"qpn\":%u,\"wr_id\":%" PRIu64 ",\"operation\":%u,\"status\":%u",
                    index, (uintptr_t)cq, wc[i].qp_num, wc[i].wr_id, op, wc[i].status);
        } else {
            wc[kept++] = wc[i];
        }
    }
    pthread_mutex_unlock(&lock);
    return kept;
}

static void attach(struct ibv_context *context) {
    pthread_mutex_lock(&lock);
    if (log_fd < 0) {
        const char *root = getenv("ORBITTE_RESTART_ROOT");
        const char *generation = getenv("ORBITTE_RESTART_GENERATION");
        if (!root || !generation || strlen(generation) != 1 || generation[0] < '0' || generation[0] > '2') fail();
        char path[4096];
        int length = snprintf(path, sizeof(path), "%s/cq-fault-%s.jsonl", root, generation);
        if (length < 0 || (size_t)length >= sizeof(path)) fail();
        log_fd = open(path, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
        if (log_fd < 0) fail();
    }
    unsigned index = 0;
    while (index < 8 && contexts[index].owner) ++index;
    if (index == 8 || !context->ops.poll_cq || !context->ops.post_send) fail();
    contexts[index] = (struct context){context, context->ops.poll_cq, context->ops.post_send};
    Dl_info provider = {0};
    if (!dladdr((void *)context->ops.poll_cq, &provider) || !provider.dli_fname) fail();
    char provider_path[4096];
    if (!realpath(provider.dli_fname, provider_path)) fail();
    receipt("\"kind\":\"context\",\"context\":%u,\"provider\":\"%s\"", index, provider_path);
    ++state.contexts;
    context->ops.poll_cq = poll;
    context->ops.post_send = post;
    pthread_mutex_unlock(&lock);
}

struct ibv_context *ibv_open_device(struct ibv_device *device) {
    pthread_once(&symbols, resolve);
    struct ibv_context *context = native_open(device);
    if (context) attach(context);
    return context;
}

int ibv_close_device(struct ibv_context *context) {
    pthread_once(&symbols, resolve);
    pthread_mutex_lock(&lock);
    unsigned index = find(context);
    context->ops.poll_cq = contexts[index].poll;
    context->ops.post_send = contexts[index].post;
    pthread_mutex_unlock(&lock);
    int code = native_close(context);
    pthread_mutex_lock(&lock);
    receipt("\"kind\":\"close_context\",\"context\":%u,\"code\":%d", index, code);
    if (code == 0) { contexts[index] = (struct context){0}; --state.contexts; }
    else { context->ops.poll_cq = poll; context->ops.post_send = post; ++state.errors; }
    pthread_mutex_unlock(&lock);
    return code;
}

/* The filter lives only in this joined helper, never in an engine worker.
 * It denies command writes/ioctls on exactly this test-owned uverbs FD. */
struct destruction { struct ibv_qp *qp; int code; };
static void *deny_destroy(void *argument) {
    struct destruction *call = argument;
    unsigned fd = (unsigned)call->qp->context->cmd_fd;
    struct sock_filter filter[] = {
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_write, 1, 0),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_ioctl, 0, 3),
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, args[0])),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, fd, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EIO),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    };
    struct sock_fprog program = {sizeof(filter) / sizeof(filter[0]), filter};
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) || prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &program)) fail();
    call->code = native_destroy_qp(call->qp);
    return NULL;
}

int ibv_destroy_qp(struct ibv_qp *qp) {
    pthread_once(&symbols, resolve);
    pthread_mutex_lock(&lock);
    unsigned index = find(qp->context), qpn = qp->qp_num;
    uintptr_t cq = (uintptr_t)qp->send_cq;
    int inject = cleanup.armed && qp == payload_qp;
    if (inject && ++cleanup.attempts != 1) fail();
    pthread_mutex_unlock(&lock);
    int code;
    if (inject) {
        pthread_t thread;
        struct destruction call = {qp, 0};
        if (pthread_create(&thread, NULL, deny_destroy, &call) || pthread_join(thread, NULL)) fail();
        code = call.code;
    } else code = native_destroy_qp(qp);
    pthread_mutex_lock(&lock);
    receipt("\"kind\":\"destroy_qp\",\"context\":%u,\"cq\":%" PRIuPTR ",\"qpn\":%u,\"code\":%d", index, cq, qpn, code);
    if (inject) {
        cleanup.code = (uint64_t)code;
        if (code != EIO) ++state.errors;
        receipt("\"kind\":\"cleanup_error\",\"context\":%u,\"cq\":%" PRIuPTR ",\"qpn\":%u,\"code\":%d,\"attempts\":%" PRIu64 ",\"syscall_fault\":true",
                index, cq, qpn, code, cleanup.attempts);
    } else if (code != 0) ++state.errors;
    inflight_destroyed((struct ibv_cq *)cq, qpn, code);
    if (code == 0) ++state.destroyed_qps;
    pthread_mutex_unlock(&lock);
    return code;
}

/* Only the installed fixture calls this ABI; OrbitTE never links this tool. */
int orbitte_cq_fault(unsigned command, unsigned phase, unsigned op, unsigned limit, struct snapshot *output) {
    if (!output) return EINVAL;
    pthread_mutex_lock(&lock);
    int error = 0;
    if (command == 1) {
        if (inflight.armed || state.armed || state.errors || state.contexts != 2 || !(op == 1 || op == 2) || !limit || limit > 32) error = EINVAL;
        else {
            state.armed = 1; state.phase = phase; state.posts = 0; state.lost = 0; state.destroyed_qps = 0;
            expected = limit; selected_operation = op;
            receipt("\"kind\":\"arm\",\"operation\":%u,\"expected\":%u", op, limit);
        }
    } else if (command == 2) {
        if (!state.armed || state.posts != expected || state.lost != expected || state.errors) error = EINVAL;
        else { state.armed = 0; receipt("\"kind\":\"disarm\""); }
    } else if (command != 0) error = EINVAL;
    *output = state;
    pthread_mutex_unlock(&lock);
    return error;
}

/* Raw QP pointers here are witnesses, never additional resource owners. */
int orbitte_cleanup_fault(unsigned command, struct cleanup_snapshot *output) {
    if (!output) return EINVAL;
    pthread_mutex_lock(&lock);
    int error = 0;
    if (command == 1) {
        if (cleanup.armed || !state.armed || expected != 1 || state.posts != 1 || state.lost != 1 || !payload_qp || state.errors) error = EINVAL;
        else {
            cleanup.armed = 1; cleanup.qpn = payload_qp->qp_num;
            receipt("\"kind\":\"arm_cleanup\",\"qpn\":%" PRIu64, cleanup.qpn);
        }
    } else if (command == 2) {
        if (cleanup.attempts != 1 || cleanup.code != EIO) error = EINVAL;
        else {
            struct ibv_qp_attr attr = {0};
            struct ibv_qp_init_attr init = {0};
            int code = native_query_qp(payload_qp, &attr, IBV_QP_STATE, &init);
            cleanup.live = code == 0 && payload_qp->qp_num == cleanup.qpn;
            receipt("\"kind\":\"query_cleanup\",\"qpn\":%" PRIu64 ",\"code\":%d,\"state\":%u", cleanup.qpn, code, (unsigned)attr.qp_state);
            if (!cleanup.live) error = EINVAL;
        }
    } else if (command != 0) error = EINVAL;
    cleanup.sequence = state.sequence;
    *output = cleanup;
    pthread_mutex_unlock(&lock);
    return error;
}
