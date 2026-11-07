/* Installed P10 control: submit to real cuFile, withhold CQ receipts until released.
 * It never manufactures completion, cancels an IO, or destroys a native object. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
typedef struct { int err, cuda; } status_t;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static void *(*lookup)(void *, const char *);
static status_t (*submit_native)(void *, unsigned, void *, unsigned);
static status_t (*poll_native)(void *, unsigned, unsigned *, void *, struct timespec *);
static int path(char *out, const char *leaf) {
    const char *root = getenv("ORBITE_GDS_P10_CONTROL");
    if (!root) return 0;
    int n = snprintf(out, PATH_MAX, "%s/%s", root, leaf);
    if (n < 0 || n >= PATH_MAX) _exit(125);
    return 1;
}
static status_t submit_wrap(void *b, unsigned n, void *params, unsigned flags) {
    status_t s = submit_native(b, n, params, flags);
    char name[PATH_MAX];
    if (!s.err && path(name, "posted")) {
        int fd = open(name, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (fd < 0 || close(fd)) _exit(125);
    }
    return s;
}
static status_t poll_wrap(void *b, unsigned min, unsigned *n, void *events, struct timespec *timeout) {
    char release[PATH_MAX];
    if (path(release, "release") && access(release, F_OK)) {
        *n = 0;
        return (status_t){0, 0};
    }
    return poll_native(b, min, n, events, timeout);
}
void *dlsym(void *library, const char *name) {
    pthread_mutex_lock(&lock);
#ifdef __aarch64__
    const char *version = "GLIBC_2.17";
#else
    const char *version = "GLIBC_2.2.5";
#endif
    if (!lookup) *(void **)(&lookup) = dlvsym(RTLD_NEXT, "dlsym", version);
    if (!lookup) _exit(125);
    void *result = lookup(library, name);
    if (result && !strcmp(name, "cuFileBatchIOSubmit")) {
        memcpy(&submit_native, &result, sizeof(result)); result = (void *)submit_wrap;
    } else if (result && !strcmp(name, "cuFileBatchIOGetStatus")) {
        memcpy(&poll_native, &result, sizeof(result)); result = (void *)poll_wrap;
    }
    pthread_mutex_unlock(&lock);
    return result;
}
