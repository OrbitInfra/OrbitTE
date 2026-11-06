/* CPU-only CUDA Driver API control for the installed wheel's production table.
 * Bounded real host allocations stand in for device addresses. No GPU claim. */
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LIMIT 128
static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t wake = PTHREAD_COND_INITIALIZER;
struct allocation { uintptr_t p; size_t n; int device; };
static struct allocation allocations[LIMIT];
static _Thread_local void *stack[32];
static _Thread_local int depth;
static _Thread_local uintptr_t pending_dst, pending_src;
static _Thread_local size_t pending_bytes;
static int hold, entered, fault, destroyed, freed, probes, ranges, records, host_frees;
static void *ctx(int d) { return (void *)(uintptr_t)(16 + d); }
static struct allocation *find(uintptr_t p) {
    for (int i=0;i<LIMIT;i++) if (allocations[i].p && p>=allocations[i].p && p-allocations[i].p<allocations[i].n) return &allocations[i];
    return NULL;
}
uintptr_t orbite_test_allocate(size_t n, int d) {
    pthread_mutex_lock(&mu);
    uintptr_t p=0;
    for(int i=0;i<LIMIT;i++) if(!allocations[i].p) {
        void *v=calloc(1,n);
        if(v) { p=(uintptr_t)v; allocations[i]=(struct allocation){p,n,d}; }
        break;
    }
    pthread_mutex_unlock(&mu); return p;
}
int orbite_test_free(uintptr_t p) {
    pthread_mutex_lock(&mu); struct allocation *a=find(p);
    if(!a || a->p!=p) { pthread_mutex_unlock(&mu); return 1; }
    free((void *)p); memset(a,0,sizeof(*a)); freed++;
    pthread_mutex_unlock(&mu); return 0;
}
void orbite_test_hold(int n) { pthread_mutex_lock(&mu); hold=n; entered=0; pthread_cond_broadcast(&wake); pthread_mutex_unlock(&mu); }
void orbite_test_fault(int n) { pthread_mutex_lock(&mu); fault=n; pthread_mutex_unlock(&mu); }
int orbite_test_count(int n) { pthread_mutex_lock(&mu); int v=n==0?entered:n==1?destroyed:n==2?freed:n==3?probes:n==4?ranges:n==5?records:host_frees; pthread_mutex_unlock(&mu); return v; }
uintptr_t orbite_test_context(void) { return depth ? (uintptr_t)stack[depth-1] : 0; }
static int failed(int n) { pthread_mutex_lock(&mu); int v=fault==n; pthread_mutex_unlock(&mu); return v; }
int cuInit(unsigned int f) { (void)f; return 0; }
int cuDeviceGet(int *out,int d) { if(d<0 || d>1)return 101; *out=d;return 0; }
int cuDeviceGetCount(int *out) { *out=2;return 0; }
int cuDeviceGetAttribute(int *out,int a,int d) { (void)a;(void)d;*out=0;return 0; }
int cuPointerSetAttribute(const void *v,int a,uint64_t p) { (void)v;(void)a;(void)p;return 0; }
int cuFlushGPUDirectRDMAWrites(int a,int b) { (void)a;(void)b;return 0; }
int cuDevicePrimaryCtxRetain(void **out,int d) { *out=ctx(d);return 0; }
int cuDevicePrimaryCtxRelease_v2(int d) { (void)d;return 0; }
int cuCtxGetCurrent(void **out) { if(failed(8))return 999; *out=depth?stack[depth-1]:NULL;return 0; }
int cuCtxSetCurrent(void *c) { if(depth)depth--; if(c)stack[depth++]=c;return 0; }
int cuCtxPushCurrent_v2(void *c) { if(depth==32)return 2;stack[depth++]=c;return failed(9)?999:0; }
int cuCtxPopCurrent_v2(void **out) { if(failed(3))return 999; if(!depth)return 1;*out=stack[--depth];return 0; }
int cuCtxSynchronize(void) { return failed(2)?999:0; }
int cuPointerGetAttributes(unsigned int n,int *attrs,void **out,uint64_t p) {
    pthread_mutex_lock(&mu); probes++; struct allocation *a=find((uintptr_t)p);
    for(unsigned int i=0;i<n;i++) {
        switch(attrs[i]) {
            case 1: *(void **)out[i]=a?ctx(a->device):NULL;break;
            case 2: *(int *)out[i]=a?2:0;break;
            case 8: *(unsigned int *)out[i]=0;break;
            case 9: *(int *)out[i]=a?a->device:-1;break;
            default: pthread_mutex_unlock(&mu);return 1;
        }
    }
    pthread_mutex_unlock(&mu); return 0;
}
int cuMemGetAddressRange_v2(uint64_t *base,size_t *size,uint64_t p) {
    pthread_mutex_lock(&mu); ranges++; struct allocation *a=find((uintptr_t)p);
    if(a) { *base=a->p;*size=a->n; }
    pthread_mutex_unlock(&mu); return a?0:1;
}
int cuDeviceGetPCIBusId(char *out,int n,int d) { snprintf(out,(size_t)n,"0000:ff:0%d.0",d);return 0; }
int cuMemAlloc_v2(uint64_t *p,size_t n) { *p=orbite_test_allocate(n,0);return *p?0:2; }
int cuMemFree_v2(uint64_t p) { if(failed(7))return 999;return orbite_test_free(p); }
int cuMemsetD8_v2(uint64_t p,unsigned char b,size_t n) { memset((void *)(uintptr_t)p,b,n);return 0; }
int cuMemHostAlloc(void **p,size_t n,unsigned int flags) { (void)flags;return posix_memalign(p,4096,n)?2:0; }
int cuMemFreeHost(void *p) { pthread_mutex_lock(&mu);host_frees++;int f=fault==5;pthread_mutex_unlock(&mu);if(f)return 999;free(p);return 0; }
int cuMemHostRegister_v2(void *p,size_t n,unsigned int flags) { (void)p;(void)n;(void)flags;return 0; }
int cuMemHostUnregister(void *p) { (void)p;return 0; }
struct stream { void *context; };
int cuStreamCreate(void **out,unsigned int flags) { (void)flags;struct stream *s=malloc(sizeof(*s));if(!s)return 2;s->context=depth?stack[depth-1]:NULL;*out=s;return 0; }
int cuStreamGetCtx(void *s,void **out) { *out=(uintptr_t)s<=2?(depth?stack[depth-1]:NULL):((struct stream *)s)->context;return *out?0:201; }
int cuStreamIsCapturing(void *s,int *out) { (void)s;*out=0;return 0; }
int cuStreamDestroy_v2(void *s) { if(failed(4))return 999;free(s);return 0; }
int cuStreamSynchronize(void *s) { (void)s;if(failed(2))return 999;if(pending_bytes){memmove((void*)pending_dst,(void*)pending_src,pending_bytes);pending_bytes=0;}return 0; }
int cuEventCreate(void **out,unsigned int flags) { (void)flags;*out=malloc(1);return *out?0:2; }
int cuEventRecord(void *e,void *s) { (void)e;(void)s;pthread_mutex_lock(&mu);records++;pthread_mutex_unlock(&mu);return 0; }
int cuEventSynchronize(void *e) {
    (void)e;pthread_mutex_lock(&mu);entered++;
    struct timespec until; clock_gettime(CLOCK_REALTIME,&until);until.tv_sec+=30;
    while(hold) if(pthread_cond_timedwait(&wake,&mu,&until)) { pthread_mutex_unlock(&mu);return 999; }
    int rc=(fault==1 || fault==2)?999:0;pthread_mutex_unlock(&mu);return rc;
}
int cuEventQuery(void *e) { (void)e;pthread_mutex_lock(&mu);int rc=hold?600:fault==1?999:0;pthread_mutex_unlock(&mu);return rc; }
int cuEventDestroy_v2(void *e) { pthread_mutex_lock(&mu);destroyed++;int f=fault==4;pthread_mutex_unlock(&mu);if(f)return 999;free(e);return 0; }
int cuStreamWaitEvent(void *s,void *e,unsigned int flags) { (void)s;(void)e;(void)flags;return 0; }
#ifndef ORBITE_PLUGIN_MISSING_COPY
int cuMemcpy(uint64_t d,uint64_t s,size_t n) { if(failed(6))return 999;if(failed(10)){if(pending_bytes)return 999;pending_dst=d;pending_src=s;pending_bytes=n;return 0;}memmove((void *)(uintptr_t)d,(void *)(uintptr_t)s,n);return 0; }
#endif
int cuMemcpyAsync(uint64_t d,uint64_t s,size_t n,void *stream) { (void)stream;memmove((void *)(uintptr_t)d,(void *)(uintptr_t)s,n);return 0; }
int cuMemcpyDtoHAsync_v2(void *d,uint64_t s,size_t n,void *stream) { return cuMemcpyAsync((uintptr_t)d,s,n,stream); }
int cuMemcpyHtoDAsync_v2(uint64_t d,const void *s,size_t n,void *stream) { return cuMemcpyAsync(d,(uintptr_t)s,n,stream); }
struct ipc_handle { char bytes[64]; };
int cuIpcGetMemHandle(struct ipc_handle *h,uint64_t p) { (void)h;(void)p;return 801; }
int cuIpcOpenMemHandle_v2(uint64_t *p,struct ipc_handle h,unsigned int flags) { (void)p;(void)h;(void)flags;return 801; }
int cuIpcCloseMemHandle(uint64_t p) { (void)p;return 801; }
