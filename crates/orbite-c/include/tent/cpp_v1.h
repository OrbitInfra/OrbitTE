/* OrbiTE source-adapter bridge. The frozen tent_request_t remains unchanged. */
#ifndef ORBITE_TENT_CPP_V1_H
#define ORBITE_TENT_CPP_V1_H
#include "tent/transfer_engine.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct {
    uint32_t struct_size;
    uint32_t abi_version;
    tent_request_t request;
    const char *policy_name;
    size_t policy_name_len;
    uint64_t deadline_ns;
    int32_t intent_type;
    uint32_t reserved;
} orbite_request_v1;
typedef struct {
    const char *name;
    size_t name_len;
    const char *message;
    size_t message_len;
} orbite_notification_v1;
/* source: 0 = environment, 1 = JSON, 2 = file path. No thread-local setters. */
tent_engine_t orbite_create_v1(uint32_t source, const char *config);
int orbite_submit_v1(tent_engine_t, tent_batch_id_t, const orbite_request_v1 *, size_t count,
                      const orbite_notification_v1 *notification, uint32_t flags);
int orbite_notify_v1(tent_engine_t, tent_segment_id_t, const orbite_notification_v1 *);
/* malloc block of name length, message length (little-endian u32s), then UTF-8 bytes per record. */
int orbite_recv_v1(tent_engine_t, uint8_t **data, size_t *size);
int orbite_progress_v1(tent_engine_t, tent_batch_id_t, tent_status_t *);
typedef struct {
    tent_status_t status;
    uint32_t drained;
    uint32_t unquiesced;
} orbite_task_v1;
int orbite_tasks_v1(tent_engine_t, tent_batch_id_t, orbite_task_v1*, size_t capacity);
int orbite_drained_v1(tent_engine_t, tent_batch_id_t);
int orbite_scatter_peer_v1(tent_engine_t, const char *name, tent_segment_id_t *id);
int orbite_abandon_v1(tent_engine_t, tent_batch_id_t);
int orbite_reap_v1(tent_engine_t, tent_batch_id_t);
int orbite_close_v1(tent_engine_t);
int orbite_probe_v1(tent_engine_t, tent_segment_id_t);
int orbite_topology_v1(tent_engine_t, char *data, size_t *size);
int orbite_limits_v1(tent_engine_t, size_t *batch_requests);
#ifdef __cplusplus
}
#endif
#endif
