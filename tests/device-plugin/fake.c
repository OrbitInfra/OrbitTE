/* Trusted CPU fixture, compiled with one fault per fresh child process. */
#include "tent/orbite_device_plugin.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef CASE
#define CASE 0
#endif
static void trace(char c) { const char *p=getenv("ORBITE_PLUGIN_TRACE"); if(p){FILE *f=fopen(p,"a");if(f){fputc(c,f);fclose(f);}} }
__attribute__((constructor)) static void loaded(void) {trace('L');}
__attribute__((destructor)) static void unloaded(void) {trace('U');}
#if CASE != 1
#if CASE == 2 || CASE == 3 || CASE == 30 || CASE == 31
const orbite_device_plugin_descriptor_v1 orbite_device_plugin_abi_v1 = {
    {'O','R','B','I','P','L','U','G'}, CASE==2?99:1, 64,
    CASE==3?1:72, CASE==30?4:8, 48, 8,
    {0,8,16,24,32,40,48,56,CASE==31?60:64,0,8,16}
};
#else
const orbite_device_plugin_descriptor_v1 orbite_device_plugin_abi_v1 = ORBITE_DEVICE_PLUGIN_DESCRIPTOR_V1;
#endif
#endif
struct context { void *p[128]; size_t n[128]; };
static void *create(void) {trace('C');return CASE==7?NULL:calloc(1,sizeof(struct context));}
static int destroy(void *ctx) {trace('D');if(CASE==9||CASE==18)return -7;free(ctx);return 0;}
static int allocate(void *ctx,void **out,size_t n,const char *location) {
    (void)location;trace('A');struct context *c=ctx;
    for(int i=0;i<128;i++)if(!c->p[i]){*out=calloc(1,n);if(!*out)return -1;c->p[i]=*out;c->n[i]=n;return CASE==11?-1:0;}
    return -1;
}
static int release(void *ctx,void *p,size_t n) {
    trace('F');if(CASE==8)return -7;struct context *c=ctx;
    for(int i=0;i<128;i++)if(c->p[i]==p && c->n[i]==n){free(p);c->p[i]=NULL;return 0;}
    return -1;
}
static int copy(void *ctx,void *dst,void *src,size_t n) {
    (void)ctx;trace('M');if(CASE==10)return TENT_DEVICE_PLUGIN_ERROR;
    if(CASE==13)return -1;memcpy(dst,src,n);return 0;
}
static int locate(void *ctx,void *addr,size_t n,location_t *out,size_t count) {
    trace('Q');if(!count)return -1;struct context *c=ctx;
    for(int i=0;i<128;i++)if(c->p[i]==addr && c->n[i]==n){*out=(location_t){.start=addr,.length=n};strcpy(out->location,"cpu:0");return CASE==12?2:1;}return 0;
}
static int count(void *ctx) {(void)ctx;return (CASE==14||CASE==18)?-1:CASE==29?65:1;}
static int pci(void *ctx,int d,char *out,size_t n) {(void)ctx;(void)d;if(CASE==15)return -1;if(CASE==19){memset(out,'x',n);return 0;}snprintf(out,n,"0000:00:00.0");return 0;}
#if CASE != 4
int tent_register_device_plugin(device_plugin_t *out) {
    trace('R');if(!out)return -1;
    *out=(device_plugin_t){"cpu",create,destroy,allocate,release,copy,locate,count,pci};
    if(CASE==5)return -1;
    if(CASE==6)memset(out,0,sizeof(*out));
    if(CASE==16)out->class_name="rocm";
    if(CASE==17)out->class_name="cuda";
    if(CASE==20)out->class_name=NULL;
    if(CASE==21)out->create_plugin=NULL;
    if(CASE==22)out->destroy_plugin=NULL;
    if(CASE==23)out->alloc=NULL;
    if(CASE==24)out->free=NULL;
    if(CASE==25)out->memcpy_sync=NULL;
    if(CASE==26)out->query_location=NULL;
    if(CASE==27)out->get_device_count=NULL;
    if(CASE==28)out->get_device_pci_bus_id=NULL;
    return 0;
}
#endif
