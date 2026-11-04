// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_API_H
#define TENT_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

#define tent_batch_id_t uint64_t
#define tent_segment_id_t uint64_t

#ifndef LOCAL_SEGMENT_ID
#define LOCAL_SEGMENT_ID (0ull)
#endif

#define OPCODE_READ (0)
#define OPCODE_WRITE (1)

/* IMPORTANT: callers MUST zero-initialize this struct
 * (e.g. `tent_request_t r = {0};`). transport_hint relies on
 * UNSPEC == 0 for the no-hint default. */
struct tent_request {
    int opcode;
    void* source;
    tent_segment_id_t target_id;
    uint64_t target_offset;
    uint64_t length;
    int priority;       /* Request priority (0=HIGH, 1=MEDIUM, 2=LOW) */
    int transport_hint; /* TRANSPORT_UNSPEC=follow policy, else pin transport */
};

typedef struct tent_request tent_request_t;

#define STATUS_WAITING (0)
#define STATUS_PENDING (1)
#define STATUS_INVALID (2)
#define STATUS_CANCELED (3)
#define STATUS_COMPLETED (4)
#define STATUS_TIMEOUT (5)
#define STATUS_FAILED (6)

struct tent_status {
    int status;
    uint64_t transferred_bytes;
};

typedef struct tent_status tent_status_t;

typedef void* tent_engine_t;

struct tent_buffer_info {
    uint64_t base, length;
    char location[64];
};

#define TYPE_MEMORY (0)
#define TYPE_FILE (1)

struct tent_segment_info {
    int type;
    int num_buffers;
    struct tent_buffer_info* buffers;
};

typedef struct tent_segment_info tent_segment_info_t;

struct tent_notifi_record {
    tent_segment_id_t handle;
    char name[256];
    char msg[4096];
};

typedef struct tent_notifi_record tent_notifi_record_t;

struct tent_notifi_info {
    int num_records;
    struct tent_notifi_record* records;
};

typedef struct tent_notifi_info tent_notifi_info;

#define PERM_LOCAL_READ_WRITE (0)
#define PERM_GLOBAL_READ_ONLY (1)
#define PERM_GLOBAL_READ_WRITE (2)

#define TRANSPORT_UNSPEC (0)
#define TRANSPORT_RDMA (1)
#define TRANSPORT_MNNVL (2)
#define TRANSPORT_SHM (3)
#define TRANSPORT_NVLINK (4)
#define TRANSPORT_GDS (5)
#define TRANSPORT_IOURING (6)
#define TRANSPORT_TCP (7)
#define TRANSPORT_ASCEND_DIRECT (8)
#define TRANSPORT_SUNRISE_LINK (9)
#define TRANSPORT_TPU (10)
#define TRANSPORT_UB (11)
#define TRANSPORT_MPCOMM (12)
#define TRANSPORT_HP_TCP (13)
#define TRANSPORT_XPU (14)
#define TRANSPORT_HYLINK (15)
#define TRANSPORT_FABRIC (16)
#define TRANSPORT_MLU (17)
#define TRANSPORT_MEMFABRIC (18)

struct tent_memory_options {
    char location[64];
    int permission;     /* PERM_LOCAL_READ_WRITE, etc. */
    int transport_type; /* TRANSPORT_RDMA, etc. */
    char shm_path[256];
    size_t shm_offset;
    int internal; /* 0 = false, nonzero = true */
};

typedef struct tent_memory_options tent_memory_options_t;

void tent_load_config_from_file(const char* path);

void tent_set_config(const char* key, const char* value);

tent_engine_t tent_create_engine();

void tent_destroy_engine(tent_engine_t engine);

int tent_segment_name(tent_engine_t engine, char* buf, size_t buf_len);

int tent_rpc_server_addr_port(tent_engine_t engine, char* addr_buf,
                              size_t buf_len, uint16_t* port);

int tent_open_segment(tent_engine_t engine, tent_segment_id_t* handle,
                      const char* segment_name);

int tent_close_segment(tent_engine_t engine, tent_segment_id_t handle);

int tent_get_segment_info(tent_engine_t engine, tent_segment_id_t handle,
                          tent_segment_info_t* info);

void tent_free_segment_info(tent_segment_info_t* info);

int tent_allocate_memory(tent_engine_t engine, void** addr, size_t size,
                         const char* location);

int tent_free_memory(tent_engine_t engine, void* addr);

int tent_register_memory(tent_engine_t engine, void* addr, size_t size);

int tent_unregister_memory(tent_engine_t engine, void* addr, size_t size);

tent_batch_id_t tent_allocate_batch(tent_engine_t engine, size_t batch_size);

int tent_free_batch(tent_engine_t engine, tent_batch_id_t batch_id);

int tent_submit(tent_engine_t engine, tent_batch_id_t batch_id,
                tent_request_t* entries, size_t count);

int tent_submit_notif(tent_engine_t engine, tent_batch_id_t batch_id,
                      tent_request_t* entries, size_t count, const char* name,
                      const char* message);

int tent_send_notifs(tent_engine_t engine, tent_segment_id_t handle,
                     const char* name, const char* message);

/* Free the previous result before reusing info. Successful empty polls return
 * zero records and a null pointer. */
int tent_recv_notifs(tent_engine_t engine, tent_notifi_info* info);

/* Release records and reset info so it can be reused or freed again. */
void tent_free_notifs(tent_notifi_info* info);

int tent_task_status(tent_engine_t engine, tent_batch_id_t batch_id,
                     size_t task_id, tent_status_t* status);

int tent_cancel_task(tent_engine_t engine, tent_batch_id_t batch_id,
                     size_t task_id);

int tent_overall_status(tent_engine_t engine, tent_batch_id_t batch_id,
                        tent_status_t* status);

int tent_available(tent_engine_t engine);

int tent_register_memory_with_perm(tent_engine_t engine, void* addr,
                                   size_t size, int permission);

int tent_register_memory_batch(tent_engine_t engine, void** addrs,
                               size_t* sizes, size_t count, int permission);

int tent_unregister_memory_batch(tent_engine_t engine, void** addrs,
                                 size_t* sizes, size_t count);

int tent_allocate_memory_ex(tent_engine_t engine, void** addr, size_t size,
                            tent_memory_options_t* opts);

int tent_register_memory_ex(tent_engine_t engine, void* addr, size_t size,
                            tent_memory_options_t* opts);

int tent_register_memory_batch_ex(tent_engine_t engine, void** addrs,
                                  size_t* sizes, size_t count,
                                  tent_memory_options_t* opts);

int tent_task_status_list(tent_engine_t engine, tent_batch_id_t batch_id,
                          tent_status_t* statuses, size_t* count);

// Only NICs currently able to carry traffic are reported (see NicLoadStats).
struct tent_nic_load_stat {
    char device_name[64];
    uint64_t inflight_bytes;
    double ewma_bandwidth_bps;
};

typedef struct tent_nic_load_stat tent_nic_load_stat_t;

int tent_get_nic_load_stats(tent_engine_t engine, tent_nic_load_stat_t* stats,
                            size_t* count);

#ifdef __cplusplus
}
#endif  // __cplusplus

#include "tent/orbit_extensions.h"
#endif
