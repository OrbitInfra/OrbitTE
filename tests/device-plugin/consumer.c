#define _POSIX_C_SOURCE 200809L
#include "tent/orbite_device_plugin.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#define N 66
static void still_loaded(void) { const char *path=getenv("ORBITE_PLUGIN_TRACE");assert(path);FILE *f=fopen(path,"r");assert(f);int c;while((c=fgetc(f))!=EOF)assert(c!='U');fclose(f); }
static tent_engine_t engine(void) { tent_engine_t e=tent_create_engine();assert(e && tent_available(e));return e; }
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
    for(;;){tent_status_t s={0};assert(!tent_overall_status(e,b,&s));if(s.status==STATUS_COMPLETED){assert(s.transferred_bytes==64);break;} assert(s.status==STATUS_WAITING||s.status==STATUS_PENDING);clock_gettime(CLOCK_MONOTONIC,&now);assert(now.tv_sec-start.tv_sec<30);nanosleep(&(struct timespec){0,100000},NULL);}
    drain_batch(e,b);
}
static void transfer(tent_engine_t e,tent_segment_id_t peer,void *local,void *remote,int op) {
    tent_batch_id_t b=tent_allocate_batch(e,1);assert(b);
    tent_request_t r={.opcode=op,.source=(char*)local+1,.target_id=peer,.target_offset=(uint64_t)(uintptr_t)remote+1,.length=64,.transport_hint=TRANSPORT_TCP};
    assert(!tent_submit(e,b,&r,1));wait_done(e,b);
}
static void held(tent_engine_t a) {
    void *dst=NULL,*source=NULL;assert(!tent_allocate_memory(a,&dst,N,"cpu:0"));memset(dst,'!',N);
    assert(!tent_register_memory(a,dst,N));tent_engine_t b=engine();assert(!tent_allocate_memory(b,&source,N,"*"));memset(source,'s',N);assert(!tent_register_memory(b,source,N));
    char name[256]={0};assert(!tent_segment_name(b,name,sizeof(name)));puts(name);fflush(stdout);
    assert(fgets(name,sizeof(name),stdin));name[strcspn(name,"\n")]=0;
    tent_segment_id_t peer;assert(!tent_open_segment(a,&peer,name));
    tent_request_t requests[2]={{OPCODE_READ,(char*)dst+2,peer,(uintptr_t)source+4,6,0,TRANSPORT_TCP},{OPCODE_WRITE,dst,peer,(uintptr_t)source,1,0,TRANSPORT_TCP}};
    tent_batch_id_t batch=tent_allocate_batch(a,2);assert(batch);assert(!tent_submit(a,batch,requests,2));
    int entered=atoi(getenv("ORBITE_TEST_ENTERED")),resume=atoi(getenv("ORBITE_TEST_RESUME"));char c=0;assert(read(entered,&c,1)==1&&c=='!');
    assert(!tent_cancel_task(a,batch,1));assert(tent_free_batch(a,batch)==-2);assert(tent_unregister_memory(a,dst,N)==-2);assert(tent_free_memory(a,dst)==-2);assert(orbite_close_device_plugins(a)==-2);still_loaded();assert(write(resume,"!",1)==1);
    struct timespec start,now;clock_gettime(CLOCK_MONOTONIC,&start);
    for(;;){tent_status_t status={0};assert(!tent_overall_status(a,batch,&status));if(status.status==STATUS_CANCELED){assert(status.transferred_bytes==6);break;}assert(status.status==STATUS_WAITING||status.status==STATUS_PENDING);clock_gettime(CLOCK_MONOTONIC,&now);assert(now.tv_sec-start.tv_sec<30);nanosleep(&(struct timespec){0,100000},NULL);}
    drain_batch(a,batch);drain_registration(a,dst,N);
    for(int i=0;i<N;i++){assert(((char*)dst)[i]==(i>=2&&i<8?'s':'!'));assert(((char*)source)[i]=='s');}
    assert(!tent_free_memory(a,dst));assert(!orbite_close_device_plugins(a));assert(!tent_close_segment(a,peer));
    drain_registration(b,source,N);assert(!tent_free_memory(b,source));tent_destroy_engine(b);tent_destroy_engine(a);
}
int main(int argc,char **argv) {
    assert(argc==3);int mode=atoi(argv[1]);const char *plugin=argv[2];
    tent_engine_t a=engine();
    if(mode==100){void *p=NULL;assert(!tent_allocate_memory(a,&p,N,"*"));assert(!tent_free_memory(a,p));tent_destroy_engine(a);return 0;}
    int loaded=orbite_load_device_plugins(a,plugin);
    if(mode==101){assert(loaded==-200);tent_destroy_engine(a);return 0;}
    if(mode==102){assert(loaded==-2);tent_destroy_engine(a);return 0;}
    if(mode==18){assert(loaded==-135);assert(orbite_close_device_plugins(a)==-135);assert(orbite_close_device_plugins(a)==-135);still_loaded();tent_destroy_engine(a);return 0;}
    if(mode==1||mode==2||mode==3||mode==4||mode==5||mode==6||mode==7||mode==14||mode==15||mode==16||mode==19||mode==29||mode==30||mode==31||(mode>=20&&mode<=28)){
        assert(loaded==-200);if(mode==16){void *excluded=NULL;assert(tent_allocate_memory(a,&excluded,N,"rocm:0")==-200);assert(tent_allocate_memory(a,&excluded,N,"hip:0")==-200);assert(!excluded);}
        tent_destroy_engine(a);return 0;
    }
    assert(!loaded);
    assert(orbite_load_device_plugins(NULL,plugin)==-1);assert(orbite_load_device_plugins(a,NULL)==-1);assert(orbite_load_device_plugins(a,plugin)==-1);
    if(mode==103){held(a);return 0;}
    if(mode==9){assert(orbite_close_device_plugins(a)==-135);assert(orbite_close_device_plugins(a)==-135);still_loaded();tent_destroy_engine(a);return 0;}
    void *p=NULL,*q=NULL;
    int allocated=tent_allocate_memory(a,&p,N,"cpu:0");
    if(mode==11){assert(allocated==-135);assert(orbite_close_device_plugins(a)==-135);tent_destroy_engine(a);return 0;}
    if(mode==12){assert(allocated==-200);assert(!orbite_close_device_plugins(a));tent_destroy_engine(a);return 0;}
    assert(!allocated);assert(p);
    if(mode==8){assert(tent_free_memory(a,p)==-135);assert(tent_free_memory(a,p)==-135);assert(orbite_close_device_plugins(a)==-135);still_loaded();tent_destroy_engine(a);return 0;}
    assert(!tent_allocate_memory(a,&q,N,"cpu:0"));
    memset(p,0x69,N);memset(q,0x72,N);
    int copied=orbite_plugin_memcpy_sync(a,(char*)q+1,(char*)p+1,64);
    if(mode==10){assert(copied==-135);for(int i=0;i<N;i++)assert(((unsigned char*)q)[i]==0x72);assert(tent_free_memory(a,p)==-135);assert(tent_free_memory(a,q)==-135);still_loaded();tent_destroy_engine(a);return 0;}
    assert(!copied);assert(((unsigned char*)q)[0]==0x72 && ((unsigned char*)q)[65]==0x72);assert(!memcmp((char*)p+1,(char*)q+1,64));
    assert(orbite_plugin_memcpy_sync(a,p,(char*)p+1,64)==-1);
    assert(orbite_close_device_plugins(a)==-2);
    assert(!tent_register_memory(a,p,N));
    tent_engine_t b=engine();void *r=NULL;assert(!tent_allocate_memory(b,&r,N,"*"));memset(r,0x27,N);assert(!tent_register_memory(b,r,N));
    char name[256]={0};assert(!tent_segment_name(a,name,sizeof(name)));tent_segment_id_t peer=0;assert(!tent_open_segment(b,&peer,name));
    transfer(b,peer,r,p,OPCODE_READ);assert(!memcmp((char*)r+1,(char*)p+1,64));assert(((unsigned char*)r)[0]==0x27&&((unsigned char*)r)[65]==0x27);
    memset((char*)r+1,0x31,64);transfer(b,peer,r,p,OPCODE_WRITE);assert(!memcmp((char*)r+1,(char*)p+1,64));assert(((unsigned char*)p)[0]==0x69&&((unsigned char*)p)[65]==0x69);
    // An alias in another engine retains the plugin's allocation and library.
    assert(!tent_register_memory(b,p,N));assert(tent_free_memory(a,p)==-2);
    drain_registration(a,p,N);assert(tent_free_memory(a,p)==-2);
    drain_registration(b,p,N);assert(!tent_free_memory(a,p));assert(!tent_free_memory(a,q));
    assert(!tent_close_segment(b,peer));drain_registration(b,r,N);assert(!tent_free_memory(b,r));
    assert(!orbite_close_device_plugins(a));assert(!orbite_close_device_plugins(a));
    tent_destroy_engine(b);tent_destroy_engine(a);return 0;
}
