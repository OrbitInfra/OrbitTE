/* Fake provider control for observer transparency, not a GPU qualification. */
#define _GNU_SOURCE
#include <assert.h>
#include <dlfcn.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
typedef struct { char reserved[64]; } ipc_handle;
static int references;
int cuMemAlloc_v2(uint64_t *out, size_t size) { assert(size == 2097152); *out = 123; return 0; }
int cuMemsetD8_v2(uint64_t ptr, unsigned char value, size_t size) { assert(ptr == 123 && !value && size == 2097152); return 0; }
int cuCtxSynchronize(void) { return 0; }
int cuIpcGetMemHandle(ipc_handle *out, uint64_t ptr) { assert(ptr == 123); memset(out, 7, sizeof(*out)); return 0; }
int cuIpcOpenMemHandle_v2(uint64_t *out, ipc_handle handle, unsigned flags) { assert(handle.reserved[0] == 7 && flags == 1); *out = 456; ++references; return 0; }
int cuIpcCloseMemHandle(uint64_t ptr) { assert(ptr == 456); return --references ? 0 : 1; }
int cuMemcpyAsync(uint64_t dst, uint64_t src, size_t size, void *stream) { assert(dst == 456 && src == 123 && size == 17 && stream == (void *)2); return 1; }
int cuEventRecord(void *event, void *stream) { assert(event == (void *)3 && stream == (void *)2); return 1; }
int cuEventSynchronize(void *event) { assert(event == (void *)3); return 700; }
int cuCtxPopCurrent_v2(void **out) { *out = (void *)4; return 201; }
int cuMemFree_v2(uint64_t ptr) { assert(ptr == 123); return 1; }
int cuMemHostAlloc(void **out, size_t size, unsigned flags) { assert(size == 151 && flags == 1); *out = (void *)0x123456789abc; return 0; }
int cuMemFreeHost(void *ptr) { assert(ptr == (void *)0x123456789abc); return 1; }
int cuMemcpyHtoDAsync_v2(uint64_t dst, const void *src, size_t size, void *stream) { assert(dst == 456 && src == (void *)0x123456789abc && size == 17 && stream == (void *)2); return 1; }
int cuMemcpyDtoHAsync_v2(void *dst, uint64_t src, size_t size, void *stream) { assert(dst == (void *)0x123456789abc && src == 456 && size == 17 && stream == (void *)2); return 700; }
int cuCtxPushCurrent_v2(void *context) { assert(context == (void *)4); return 201; }
int cuStreamCreate(void **out, unsigned flags) { assert(flags == 1); *out = (void *)2; return 0; }
int cuEventCreate(void **out, unsigned flags) { assert(flags == 3); *out = (void *)3; return 0; }
int cuStreamDestroy_v2(void *stream) { assert(stream == (void *)2); return 1; }
int cuEventDestroy_v2(void *event) { assert(event == (void *)3); return 700; }
#define LOAD(name) __typeof__(&name) name##_fn = (__typeof__(&name))dlsym(library, #name); assert(name##_fn)
int main(void) {
    void *library = dlopen(NULL, RTLD_NOW); assert(library);
    LOAD(cuMemAlloc_v2); LOAD(cuMemsetD8_v2); LOAD(cuCtxSynchronize); LOAD(cuIpcGetMemHandle);
    LOAD(cuIpcOpenMemHandle_v2); LOAD(cuIpcCloseMemHandle); LOAD(cuMemcpyAsync);
    LOAD(cuEventRecord); LOAD(cuEventSynchronize); LOAD(cuCtxPopCurrent_v2); LOAD(cuMemFree_v2);
    uint64_t ptr=0, mapped=0; ipc_handle handle = {{0}}; void *context=NULL;
    assert(cuMemAlloc_v2_fn(&ptr,2097152)==0 && ptr==123);
    assert(cuMemsetD8_v2_fn(ptr,0,2097152)==0 && cuCtxSynchronize_fn()==0);
    assert(cuIpcGetMemHandle_fn(&handle,ptr)==0);
    assert(cuIpcOpenMemHandle_v2_fn(&mapped,handle,1)==0 && mapped==456);
    assert(cuIpcOpenMemHandle_v2_fn(&mapped,handle,1)==0 && references==2);
    assert(cuMemcpyAsync_fn(mapped,ptr,17,(void *)2)==1);
    assert(cuEventRecord_fn((void *)3,(void *)2)==1 && cuEventSynchronize_fn((void *)3)==700);
    assert(cuIpcCloseMemHandle_fn(mapped)==0 && cuIpcCloseMemHandle_fn(mapped)==1);
    assert(cuCtxPopCurrent_v2_fn(&context)==201 && context==(void *)4);
    assert(cuMemFree_v2_fn(ptr)==1);
    LOAD(cuMemHostAlloc); LOAD(cuMemFreeHost); LOAD(cuMemcpyHtoDAsync_v2); LOAD(cuMemcpyDtoHAsync_v2);
    void *host=NULL;
    assert(cuMemHostAlloc_fn(&host,151,1)==0 && host==(void *)0x123456789abc);
    assert(cuMemcpyHtoDAsync_v2_fn(456,host,17,(void *)2)==1);
    assert(cuMemcpyDtoHAsync_v2_fn(host,456,17,(void *)2)==700);
    assert(cuMemFreeHost_fn(host)==1);
    assert(setenv("ORBITTE_STAGING_OBSERVE", "1", 1)==0);
    LOAD(cuCtxPushCurrent_v2); LOAD(cuStreamCreate); LOAD(cuEventCreate);
    LOAD(cuStreamDestroy_v2); LOAD(cuEventDestroy_v2);
    void *stream=NULL, *event=NULL;
    assert(cuCtxPushCurrent_v2_fn((void *)4)==201);
    assert(cuStreamCreate_fn(&stream,1)==0 && stream==(void *)2);
    assert(cuEventCreate_fn(&event,3)==0 && event==(void *)3);
    assert(cuEventDestroy_v2_fn(event)==700 && cuStreamDestroy_v2_fn(stream)==1);
    assert(dlclose(library)==0);
    return 0;
}
