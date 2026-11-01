/* Native CUDA call observer and bounded close-delay control. Never copies data
 * or fabricates driver success/failure. No CUDA toolkit dependency. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

typedef struct { char reserved[64]; } ipc_handle;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static uint64_t sequence;
static int log_fd = -1, held;
static void *(*lookup)(void *, const char *);
static _Atomic(int (*)(uint64_t *, size_t)) allocate_native;
static _Atomic(int (*)(uint64_t)) free_native;
static _Atomic(int (*)(void **, size_t, unsigned)) pin_native;
static _Atomic(int (*)(void *)) unpin_native;
static _Atomic(int (*)(uint64_t, const void *, size_t, void *)) htod_native;
static _Atomic(int (*)(void *, uint64_t, size_t, void *)) dtoh_native;
static _Atomic(int (*)(ipc_handle *, uint64_t)) export_native;
static _Atomic(int (*)(uint64_t *, ipc_handle, unsigned)) open_native;
static _Atomic(int (*)(uint64_t)) close_native;
static _Atomic(int (*)(uint64_t, uint64_t, size_t, void *)) copy_native;
static _Atomic(int (*)(void *, void *)) record_native;
static _Atomic(int (*)(void *)) wait_native, query_native;
static _Atomic(int (*)(void **)) pop_native;
static _Atomic(int (*)(void *)) push_native;
static _Atomic(int (*)(void **, unsigned)) stream_create_native, event_create_native;
static _Atomic(int (*)(void *)) stream_destroy_native, event_destroy_native;
static _Atomic(int (*)(uint64_t, unsigned char, size_t)) memset_native;
static _Atomic(int (*)(void)) sync_native;

static void emit(const char *kind, const char *format, ...) {
    pthread_mutex_lock(&lock);
    if (log_fd < 0) {
        const char *path = getenv("ORBITTE_IPC_LOG");
        if (!path || (log_fd = open(path, O_WRONLY|O_CREAT|O_APPEND|O_CLOEXEC, 0600)) < 0) _exit(125);
    }
    char fields[768], line[1024];
    va_list args; va_start(args, format); int size = vsnprintf(fields, sizeof(fields), format, args); va_end(args);
    if (size < 0 || (size_t)size >= sizeof(fields)) _exit(125);
    size = snprintf(line, sizeof(line), "{\"seq\":%" PRIu64 ",\"pid\":%ld,\"tid\":%ld,\"kind\":\"%s\",%s}\n",
                    ++sequence, (long)getpid(), (long)syscall(SYS_gettid), kind, fields);
    if (size < 0 || (size_t)size >= sizeof(line) || write(log_fd, line, (size_t)size) != size) _exit(125);
    pthread_mutex_unlock(&lock);
}

static uint64_t hash_handle(ipc_handle handle) {
    uint64_t hash = UINT64_C(14695981039346656037);
    for (size_t index = 0; index < sizeof(handle); ++index) { hash ^= (unsigned char)handle.reserved[index]; hash *= UINT64_C(1099511628211); }
    return hash;
}
static int staging(void);
static int allocate_wrap(uint64_t *address, size_t length) {
    int code = allocate_native(address, length);
    emit("allocate", "\"address\":%" PRIu64 ",\"length\":%zu,\"code\":%d", *address, length, code); return code;
}
static int memset_wrap(uint64_t address, unsigned char value, size_t length) {
    int code = memset_native(address, value, length);
    emit("memset", "\"address\":%" PRIu64 ",\"value\":%u,\"length\":%zu,\"code\":%d", address, value, length, code); return code;
}
static int sync_wrap(void) {
    int code = sync_native(); emit("sync", "\"code\":%d", code); return code;
}
static int free_wrap(uint64_t address) {
    if (staging()) emit("free_begin", "\"address\":%" PRIu64 ",\"code\":%d", address, 0);
    int code = free_native(address); emit("free", "\"address\":%" PRIu64 ",\"code\":%d", address, code); return code;
}
static int pin_wrap(void **address, size_t length, unsigned flags) {
    int code = pin_native(address, length, flags);
    emit("pin", "\"address\":%" PRIuPTR ",\"length\":%zu,\"flags\":%u,\"code\":%d", (uintptr_t)*address, length, flags, code); return code;
}
static int unpin_wrap(void *address) {
    if (staging()) emit("unpin_begin", "\"address\":%" PRIuPTR ",\"code\":%d", (uintptr_t)address, 0);
    int code = unpin_native(address);
    emit("unpin", "\"address\":%" PRIuPTR ",\"code\":%d", (uintptr_t)address, code); return code;
}
static int htod_wrap(uint64_t destination, const void *source, size_t length, void *stream) {
    if (staging()) emit("htod_begin", "\"source\":%" PRIuPTR ",\"destination\":%" PRIu64 ",\"length\":%zu,\"stream\":%" PRIuPTR ",\"code\":%d", (uintptr_t)source, destination, length, (uintptr_t)stream, 0);
    int code = htod_native(destination, source, length, stream);
    emit("htod", "\"source\":%" PRIuPTR ",\"destination\":%" PRIu64 ",\"length\":%zu,\"stream\":%" PRIuPTR ",\"code\":%d", (uintptr_t)source, destination, length, (uintptr_t)stream, code); return code;
}
static int dtoh_wrap(void *destination, uint64_t source, size_t length, void *stream) {
    if (staging()) emit("dtoh_begin", "\"source\":%" PRIu64 ",\"destination\":%" PRIuPTR ",\"length\":%zu,\"stream\":%" PRIuPTR ",\"code\":%d", source, (uintptr_t)destination, length, (uintptr_t)stream, 0);
    int code = dtoh_native(destination, source, length, stream);
    emit("dtoh", "\"source\":%" PRIu64 ",\"destination\":%" PRIuPTR ",\"length\":%zu,\"stream\":%" PRIuPTR ",\"code\":%d", source, (uintptr_t)destination, length, (uintptr_t)stream, code); return code;
}
static int export_wrap(ipc_handle *handle, uint64_t address) {
    int code = export_native(handle, address);
    emit("export", "\"address\":%" PRIu64 ",\"handle\":%" PRIu64 ",\"code\":%d", address, hash_handle(*handle), code); return code;
}
static int open_wrap(uint64_t *address, ipc_handle handle, unsigned flags) {
    int code = open_native(address, handle, flags);
    emit("open", "\"address\":%" PRIu64 ",\"handle\":%" PRIu64 ",\"flags\":%u,\"code\":%d", *address, hash_handle(handle), flags, code); return code;
}
static int close_wrap(uint64_t address) {
    const char *hold = getenv("ORBITTE_IPC_HOLD");
    if (hold && !__atomic_exchange_n(&held, 1, __ATOMIC_RELAXED)) {
        char ready[4096]; if (snprintf(ready, sizeof(ready), "%s.ready", hold) >= (int)sizeof(ready)) _exit(125);
        int fd = open(ready, O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC, 0600); if (fd < 0) _exit(125); close(fd);
        struct timespec start, now, delay = {.tv_sec=0, .tv_nsec=1000000}; clock_gettime(CLOCK_MONOTONIC, &start);
        emit("hold", "\"address\":%" PRIu64 ",\"code\":0", address);
        while (access(hold, F_OK)) {
            clock_gettime(CLOCK_MONOTONIC, &now); if (now.tv_sec - start.tv_sec >= 25) _exit(124);
            nanosleep(&delay, NULL);
        }
    }
    int code = close_native(address); emit("close", "\"address\":%" PRIu64 ",\"code\":%d", address, code); return code;
}
static int copy_wrap(uint64_t destination, uint64_t source, size_t length, void *stream) {
    int code = copy_native(destination, source, length, stream);
    emit("copy", "\"source\":%" PRIu64 ",\"destination\":%" PRIu64 ",\"length\":%zu,\"stream\":%" PRIuPTR ",\"code\":%d", source, destination, length, (uintptr_t)stream, code); return code;
}
static int record_wrap(void *event, void *stream) {
    int code = record_native(event, stream);
    emit("record", "\"event\":%" PRIuPTR ",\"stream\":%" PRIuPTR ",\"code\":%d", (uintptr_t)event, (uintptr_t)stream, code); return code;
}
static int wait_wrap(void *event) {
    if (staging()) emit("wait_begin", "\"event\":%" PRIuPTR ",\"code\":%d", (uintptr_t)event, 0);
    int code = wait_native(event); emit("wait", "\"event\":%" PRIuPTR ",\"code\":%d", (uintptr_t)event, code); return code;
}
static int query_wrap(void *event) {
    if (staging()) emit("query_begin", "\"event\":%" PRIuPTR ",\"code\":0", (uintptr_t)event);
    int code = query_native(event);
    emit("query", "\"event\":%" PRIuPTR ",\"code\":%d", (uintptr_t)event, code);
    return code;
}
static int pop_wrap(void **context) {
    int code = pop_native(context); emit("pop", "\"context\":%" PRIuPTR ",\"code\":%d", (uintptr_t)*context, code); return code;
}

/* Extra lifecycle records are opt-in so the IPC observer keeps its contract. */
static int staging(void) {
    const char *value = getenv("ORBITTE_STAGING_OBSERVE");
    return value && !strcmp(value, "1");
}
static int push_wrap(void *context) {
    int code = push_native(context);
    if (staging()) emit("push", "\"context\":%" PRIuPTR ",\"code\":%d", (uintptr_t)context, code);
    return code;
}
#define CREATE_WRAP(name, field) \
static int name##_wrap(void **handle, unsigned flags) { \
    int code = name##_native(handle, flags); \
    if (staging()) emit(#name, "\"" #field "\":%" PRIuPTR ",\"flags\":%u,\"code\":%d", (uintptr_t)*handle, flags, code); \
    return code; \
}
#define DESTROY_WRAP(name, field) \
static int name##_wrap(void *handle) { \
    if (staging()) emit(#name "_begin", "\"" #field "\":%" PRIuPTR ",\"code\":0", (uintptr_t)handle); \
    int code = name##_native(handle); \
    if (staging()) emit(#name, "\"" #field "\":%" PRIuPTR ",\"code\":%d", (uintptr_t)handle, code); \
    return code; \
}
CREATE_WRAP(stream_create, stream)
CREATE_WRAP(event_create, event)
DESTROY_WRAP(stream_destroy, stream)
DESTROY_WRAP(event_destroy, event)
#undef CREATE_WRAP
#undef DESTROY_WRAP

void *dlsym(void *library, const char *name) {
    pthread_mutex_lock(&lock);
    if (!lookup) *(void **)(&lookup) = dlvsym(RTLD_NEXT, "dlsym", "GLIBC_2.2.5");
    if (!lookup) _exit(125);
    void *result = lookup(library, name);
#define WRAP(symbol, field, wrapper) if (result && !strcmp(name, symbol)) { __typeof__(atomic_load(&field)) target; memcpy(&target, &result, sizeof(target)); atomic_store(&field, target); result = (void *)(wrapper); }
    WRAP("cuMemAlloc_v2", allocate_native, allocate_wrap)
    WRAP("cuMemFree_v2", free_native, free_wrap)
    WRAP("cuMemHostAlloc", pin_native, pin_wrap)
    WRAP("cuMemFreeHost", unpin_native, unpin_wrap)
    WRAP("cuMemcpyHtoDAsync_v2", htod_native, htod_wrap)
    WRAP("cuMemcpyDtoHAsync_v2", dtoh_native, dtoh_wrap)
    WRAP("cuMemsetD8_v2", memset_native, memset_wrap)
    WRAP("cuCtxSynchronize", sync_native, sync_wrap)
    WRAP("cuIpcGetMemHandle", export_native, export_wrap)
    WRAP("cuIpcOpenMemHandle_v2", open_native, open_wrap)
    WRAP("cuIpcCloseMemHandle", close_native, close_wrap)
    WRAP("cuMemcpyAsync", copy_native, copy_wrap)
    WRAP("cuEventRecord", record_native, record_wrap)
    WRAP("cuEventSynchronize", wait_native, wait_wrap)
    WRAP("cuEventQuery", query_native, query_wrap)
    WRAP("cuCtxPopCurrent_v2", pop_native, pop_wrap)
    WRAP("cuCtxPushCurrent_v2", push_native, push_wrap)
    WRAP("cuStreamCreate", stream_create_native, stream_create_wrap)
    WRAP("cuEventCreate", event_create_native, event_create_wrap)
    WRAP("cuStreamDestroy_v2", stream_destroy_native, stream_destroy_wrap)
    WRAP("cuEventDestroy_v2", event_destroy_native, event_destroy_wrap)
#undef WRAP
    pthread_mutex_unlock(&lock);
    return result;
}
