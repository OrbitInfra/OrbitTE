/* Installed CUDA plugin gate. Never infer GPU qualification from a shim run. */
#define _POSIX_C_SOURCE 200809L
#include "tent/orbite_device_plugin.h"
#include <assert.h>
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#define N 4098
static void drain_batch(tent_engine_t e,tent_batch_id_t batch) {
    struct timespec start,now;clock_gettime(CLOCK_MONOTONIC,&start);
    for(;;){int rc=tent_free_batch(e,batch);if(!rc)return;assert(rc==-2);clock_gettime(CLOCK_MONOTONIC,&now);assert(now.tv_sec-start.tv_sec<30);nanosleep(&(struct timespec){0,100000},NULL);}
}
static void drain_registration(tent_engine_t e,void *ptr,size_t n) {
    struct timespec start,now;clock_gettime(CLOCK_MONOTONIC,&start);
    for(;;){int rc=tent_unregister_memory(e,ptr,n);if(!rc)return;assert(rc==-2);clock_gettime(CLOCK_MONOTONIC,&now);assert(now.tv_sec-start.tv_sec<30);nanosleep(&(struct timespec){0,100000},NULL);}
}
static void wait_done(tent_engine_t e,tent_batch_id_t b) {
    struct timespec start,now;clock_gettime(CLOCK_MONOTONIC,&start);
    for(;;){tent_status_t s={0};assert(!tent_overall_status(e,b,&s));if(s.status==STATUS_COMPLETED){assert(s.transferred_bytes==N-2);break;}assert(s.status==STATUS_WAITING||s.status==STATUS_PENDING);clock_gettime(CLOCK_MONOTONIC,&now);assert(now.tv_sec-start.tv_sec<30);nanosleep(&(struct timespec){0,100000},NULL);}
    drain_batch(e,b);
}
int main(int argc,char **argv) {
    assert(argc==2||argc==3);int fault=argc==3?atoi(argv[2]):0;
    tent_engine_t a=tent_create_engine(),b=tent_create_engine();assert(tent_available(a)&&tent_available(b));
    if(fault==99){assert(orbite_load_device_plugins(a,argv[1])==-200);tent_destroy_engine(a);tent_destroy_engine(b);return 0;}
    assert(!orbite_load_device_plugins(a,argv[1]));
    void *driver=dlopen("libcuda.so.1",RTLD_NOW|RTLD_LOCAL);assert(driver);
    int (*get)(void**)=dlsym(driver,"cuCtxGetCurrent");assert(get);
    void (*inject)(int)=dlsym(driver,"orbite_test_fault");
    int (*set)(void*)=dlsym(driver,"cuCtxSetCurrent");assert(set);
    uintptr_t (*current)(void)=dlsym(driver,"orbite_test_context");
    void *caller=NULL;assert(!get(&caller));
    if(current && (fault==20 || fault==26)){assert(!set((void*)(uintptr_t)0x1234));assert(!get(&caller));}
    void *gpu=NULL,*host=NULL,*remote=NULL;
    assert(!tent_allocate_memory(a,&host,N,"*"));assert(!tent_allocate_memory(a,&gpu,N,"cuda:0"));
    void *after=NULL;assert(!get(&after));assert(after==caller);
    for(int i=0;i<N;i++)((unsigned char*)host)[i]=(unsigned char)(i*31+7);
    if(inject && fault==22){
        tent_memory_options_t options={0};options.transport_type=TRANSPORT_SHM;options.permission=PERM_GLOBAL_READ_WRITE;
        void *shared=NULL;assert(!tent_allocate_memory_ex(a,&shared,N,&options));memset(shared,0x77,N);
        assert(orbite_plugin_memcpy_sync(a,shared,host,N)==-200);for(int i=0;i<N;i++)assert(((unsigned char*)shared)[i]==0x77);
        assert(!tent_free_memory(a,shared));assert(!tent_free_memory(a,gpu));assert(!tent_free_memory(a,host));assert(!orbite_close_device_plugins(a));tent_destroy_engine(a);tent_destroy_engine(b);dlclose(driver);return 0;
    }
    if(inject && fault==21){
        tent_register_device_plugin_fn registration=dlsym(dlopen(argv[1],RTLD_NOW|RTLD_LOCAL),TENT_DEVICE_PLUGIN_REGISTER_SYMBOL);assert(registration);
        device_plugin_t table={0};assert(registration(NULL)==-1);assert(!registration(&table));void *ctx=table.create_plugin();assert(ctx);assert(table.memcpy_sync(NULL,host,host,N)==TENT_DEVICE_PLUGIN_ERROR);
        uintptr_t (*external)(size_t,int)=dlsym(driver,"orbite_test_allocate");int (*release)(uintptr_t)=dlsym(driver,"orbite_test_free");assert(external&&release);
        uintptr_t address=external(N,0);assert(address);location_t loc={0};assert(table.query_location(ctx,(void*)address,N,&loc,1)==1);assert(loc.start==(void*)address&&loc.length==N&&!strcmp(loc.location,"cuda:0"));
        memset(host,0x44,N);assert(table.memcpy_sync(ctx,host,(void*)address,N)==TENT_DEVICE_PLUGIN_ERROR);for(int i=0;i<N;i++)assert(((unsigned char*)host)[i]==0x44);
        inject(8);assert(table.memcpy_sync(ctx,host,host,N)==-1);inject(0);
        assert(!table.destroy_plugin(ctx));assert(!release(address));assert(!tent_free_memory(a,gpu));assert(!tent_free_memory(a,host));assert(!orbite_close_device_plugins(a));tent_destroy_engine(a);tent_destroy_engine(b);dlclose(driver);return 0;
    }
    if(inject && fault && fault!=20 && fault!=7)inject(fault==26?6:fault==23?10:fault);
    int copied=orbite_plugin_memcpy_sync(a,gpu,host,N);
    if(fault && fault!=20 && fault!=7 && fault!=23){assert(copied==-135);inject(0);assert(!get(&after));assert(after==caller);assert(tent_free_memory(a,gpu)==-135);assert(tent_free_memory(a,host)==-135);assert(orbite_close_device_plugins(a)==-135);tent_destroy_engine(a);tent_destroy_engine(b);dlclose(driver);return 0;}
    assert(!copied);assert(!get(&after));assert(after==caller);
    if(fault==23){
        void *second=NULL;assert(!tent_allocate_memory(a,&second,N,"cuda:0"));
        assert(!orbite_plugin_memcpy_sync(a,second,gpu,N));memset(host,0,N);
        assert(!orbite_plugin_memcpy_sync(a,host,second,N));
        for(int i=0;i<N;i++)assert(((unsigned char*)host)[i]==(unsigned char)(i*31+7));
        assert(!tent_free_memory(a,second));
    }
    memset(host,0,N);assert(!orbite_plugin_memcpy_sync(a,host,gpu,N));
    for(int i=0;i<N;i++)assert(((unsigned char*)host)[i]==(unsigned char)(i*31+7));
    assert(!tent_register_memory(a,gpu,N));assert(tent_free_memory(a,gpu)==-2);
    assert(!tent_allocate_memory(b,&remote,N,"*"));memset(remote,0xee,N);assert(!tent_register_memory(b,remote,N));
    char name[256]={0};assert(!tent_segment_name(a,name,sizeof(name)));tent_segment_id_t peer;assert(!tent_open_segment(b,&peer,name));
    for(int op=0;op<2;op++){
        if(op)memset((char*)remote+1,0x35,N-2);
        tent_request_t r={.opcode=op,.source=(char*)remote+1,.target_id=peer,.target_offset=(uintptr_t)gpu+1,.length=N-2,.transport_hint=TRANSPORT_TCP};
        tent_batch_id_t batch=tent_allocate_batch(b,1);assert(batch);assert(!tent_submit(b,batch,&r,1));wait_done(b,batch);
        if(!op)assert(!memcmp((char*)remote+1,(char*)host+1,N-2));
        assert(((unsigned char*)remote)[0]==0xee&&((unsigned char*)remote)[N-1]==0xee);
    }
    drain_registration(a,gpu,N);assert(!orbite_plugin_memcpy_sync(a,host,gpu,N));
    for(int i=1;i<N-1;i++)assert(((unsigned char*)host)[i]==0x35);
    assert(((unsigned char*)host)[0]==7 && ((unsigned char*)host)[N-1]==(unsigned char)((N-1)*31+7));
    assert(!get(&after));assert(after==caller);
    assert(!tent_close_segment(b,peer));drain_registration(b,remote,N);assert(!tent_free_memory(b,remote));if(fault==7){assert(inject);inject(7);assert(tent_free_memory(a,gpu)==-135);inject(0);assert(tent_free_memory(a,gpu)==-135);assert(orbite_close_device_plugins(a)==-135);tent_destroy_engine(a);tent_destroy_engine(b);dlclose(driver);return 0;}
    assert(!tent_free_memory(a,gpu));assert(!tent_free_memory(a,host));assert(!orbite_close_device_plugins(a));
    tent_destroy_engine(a);tent_destroy_engine(b);dlclose(driver);
    puts("device plugin: H2D/D2H, exact context, TCP READ/WRITE, guards and drained cleanup passed");
}
