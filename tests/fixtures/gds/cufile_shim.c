// Minimal Linux cuFile DSO for the loader/ABI control. All bytes are host bytes.
#define _GNU_SOURCE
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct { int err, cuda; } status_t;
typedef struct {
    unsigned major, minor; size_t poll_kb, direct_kb;
    unsigned status_flags, control_flags, features, cache, per_buffer, pinned, max_batch, ms;
} props_t;
typedef struct { int type; union { int fd; void *ptr; } handle; void *ops; } desc_t;
typedef struct {
    unsigned mode; struct { void *base; off_t file, offset; size_t size; } batch;
    void *handle; unsigned opcode; void *cookie;
} param_t;
typedef struct { void *cookie; unsigned status; size_t bytes; } event_t;
typedef struct { unsigned count, depth; event_t events[]; } batch_t;
static status_t result(bool ok) { return (status_t){ok ? 0 : 5035, 0}; }
static bool setting(const char *s) { const char *v=getenv("ORBITE_CUFILE_SHIM"); return v && !strcmp(v,s); }

status_t cuFileDriverOpen(void) { return result(!setting("open-error")); }
status_t cuFileDriverGetProperties(props_t *p) {
    *p=(props_t){.control_flags=setting("control") ? 2U : 1U,.per_buffer=4096};
    return result(true);
}
status_t cuFileGetParameterBool(int id, bool *value) {
    *value=(id==1 && setting("allow")) || (id==2 && setting("force"));
    return result(id==1 || id==2);
}
status_t cuFileHandleRegister(void **out, desc_t *d) {
    if (d->type!=1 || d->ops || !(fcntl(d->handle.fd,F_GETFL)&O_DIRECT)) return result(false);
    char name[64]; snprintf(name,sizeof(name),"/proc/self/fd/%d",d->handle.fd);
    int fd=open(name,O_RDWR); if (fd<0) return result(false);
    *out=(void *)(uintptr_t)(fd+1); return result(true);
}
void cuFileHandleDeregister(void *h) { close((int)(uintptr_t)h-1); }
status_t cuFileBufRegister(const void *p,size_t n,int f) { return result(p && n && !f); }
status_t cuFileBufDeregister(const void *p) { return result(p!=NULL); }
status_t cuFileBatchIOSetUp(void **out,unsigned depth) {
    batch_t *b=calloc(1,sizeof(*b)+depth*sizeof(event_t));
    if (!b) return result(false);
    b->depth=depth; *out=b; return result(true);
}
status_t cuFileBatchIOSubmit(void *h,unsigned n,param_t *p,unsigned flags) {
    batch_t *b=h; if (n>b->depth || flags) return result(false);
    for(unsigned i=0;i<n;i++) {
        if (p[i].mode!=1 || p[i].opcode>1) return result(false);
        int fd=(int)(uintptr_t)p[i].handle-1; char *buf=(char *)p[i].batch.base+p[i].batch.offset;
        ssize_t bytes=p[i].opcode ? pwrite(fd,buf,p[i].batch.size,p[i].batch.file) : pread(fd,buf,p[i].batch.size,p[i].batch.file);
        b->events[i]=(event_t){p[i].cookie,bytes<0?64:16,(size_t)bytes};
    }
    b->count=n;return result(true);
}
status_t cuFileBatchIOGetStatus(void *h,unsigned min,unsigned *nr,event_t *out,struct timespec *ts) {
    batch_t *b=h;if (min || ts || *nr<b->count) return result(false);
    *nr=b->count;memcpy(out,b->events,*nr*sizeof(*out));b->count=0;return result(true);
}
status_t cuFileBatchIOCancel(void *h) { return result(h!=NULL); }
void cuFileBatchIODestroy(void *h) { free(h); }
