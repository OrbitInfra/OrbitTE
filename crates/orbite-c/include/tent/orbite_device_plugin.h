/* OrbiTE checked loader extension. The frozen device_plugin.h is unchanged. */
#ifndef ORBITE_DEVICE_PLUGIN_H
#define ORBITE_DEVICE_PLUGIN_H
#include "device_plugin.h"
#include "transfer_engine.h"
#ifdef __cplusplus
extern "C" {
#endif
/* Native code remains trusted. This descriptor rejects accidental ABI mismatch
 * before calling registration. Export it as a data symbol with this exact name. */
typedef struct orbite_device_plugin_descriptor_v1 {
    char magic[8];
    uint32_t revision, pointer_bits, table_size, table_align;
    uint32_t location_size, location_align;
    uint32_t offsets[12];
} orbite_device_plugin_descriptor_v1;
#ifdef __cplusplus
#define ORBITE_PLUGIN_ALIGNOF(T) alignof(T)
#else
#define ORBITE_PLUGIN_ALIGNOF(T) _Alignof(T)
#endif
#define ORBITE_DEVICE_PLUGIN_DESCRIPTOR_V1 { \
    {'O','R','B','I','P','L','U','G'}, 1, sizeof(void*) * 8, \
    sizeof(device_plugin_t), ORBITE_PLUGIN_ALIGNOF(device_plugin_t), \
    sizeof(location_t), ORBITE_PLUGIN_ALIGNOF(location_t), { \
    offsetof(device_plugin_t,class_name), offsetof(device_plugin_t,create_plugin), \
    offsetof(device_plugin_t,destroy_plugin), offsetof(device_plugin_t,alloc), \
    offsetof(device_plugin_t,free), offsetof(device_plugin_t,memcpy_sync), \
    offsetof(device_plugin_t,query_location), offsetof(device_plugin_t,get_device_count), \
    offsetof(device_plugin_t,get_device_pci_bus_id), offsetof(location_t,start), \
    offsetof(location_t,length), offsetof(location_t,location) } }
extern const orbite_device_plugin_descriptor_v1 orbite_device_plugin_abi_v1;
/* Explicit opt-in. Load a .so or directory (up to eight .so files), once, before
 * any allocation/registration. No implicit directory, config key or env alias.
 * Negative native status codes; -135 is sticky Unquiesced, never retry cleanup. */
int orbite_load_device_plugins(tent_engine_t engine, const char* path);
/* Close only after every plugin allocation and registration has drained. A busy
 * close returns -2. A failed destroy returns -135 and keeps the library loaded. */
int orbite_close_device_plugins(tent_engine_t engine);
/* Synchronous plugin copy for engine-owned allocations. Other endpoints must
 * also be engine-owned. No host fallback after a handled device error. */
int orbite_plugin_memcpy_sync(tent_engine_t engine, void* dst, void* src, size_t size);
#ifdef __cplusplus
}
#endif
#endif
