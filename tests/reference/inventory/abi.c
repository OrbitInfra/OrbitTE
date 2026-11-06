/* Linux LP64 layout observation. Uses only the frozen public C headers. */
#include <stddef.h>
#include <stdio.h>
#include "tent/transfer_engine.h"
#ifndef ORBITE_C_API_ONLY
#include "tent/device_plugin.h"
#endif

#define RECORD(T) printf("%s\t%zu\t%zu", #T, sizeof(T), _Alignof(T))
#define FIELD(T, F) \
    printf("\t%s=%zu/%zu", #F, offsetof(T, F), sizeof(((T*)0)->F))
#define END() putchar('\n')

int main(void) {
    RECORD(tent_request_t);
    FIELD(tent_request_t, opcode);
    FIELD(tent_request_t, source);
    FIELD(tent_request_t, target_id);
    FIELD(tent_request_t, target_offset);
    FIELD(tent_request_t, length);
    FIELD(tent_request_t, priority);
    FIELD(tent_request_t, transport_hint);
    END();
    RECORD(tent_status_t);
    FIELD(tent_status_t, status);
    FIELD(tent_status_t, transferred_bytes);
    END();
    RECORD(struct tent_buffer_info);
    FIELD(struct tent_buffer_info, base);
    FIELD(struct tent_buffer_info, length);
    FIELD(struct tent_buffer_info, location);
    END();
    RECORD(tent_segment_info_t);
    FIELD(tent_segment_info_t, type);
    FIELD(tent_segment_info_t, num_buffers);
    FIELD(tent_segment_info_t, buffers);
    END();
    RECORD(tent_notifi_record_t);
    FIELD(tent_notifi_record_t, handle);
    FIELD(tent_notifi_record_t, name);
    FIELD(tent_notifi_record_t, msg);
    END();
    RECORD(tent_notifi_info);
    FIELD(tent_notifi_info, num_records);
    FIELD(tent_notifi_info, records);
    END();
    RECORD(tent_memory_options_t);
    FIELD(tent_memory_options_t, location);
    FIELD(tent_memory_options_t, permission);
    FIELD(tent_memory_options_t, transport_type);
    FIELD(tent_memory_options_t, shm_path);
    FIELD(tent_memory_options_t, shm_offset);
    FIELD(tent_memory_options_t, internal);
    END();
    RECORD(tent_nic_load_stat_t);
    FIELD(tent_nic_load_stat_t, device_name);
    FIELD(tent_nic_load_stat_t, inflight_bytes);
    FIELD(tent_nic_load_stat_t, ewma_bandwidth_bps);
    END();
#ifndef ORBITE_C_API_ONLY
    RECORD(location_t);
    FIELD(location_t, start);
    FIELD(location_t, length);
    FIELD(location_t, location);
    END();
    RECORD(device_plugin_t);
    FIELD(device_plugin_t, class_name);
    FIELD(device_plugin_t, create_plugin);
    FIELD(device_plugin_t, destroy_plugin);
    FIELD(device_plugin_t, alloc);
    FIELD(device_plugin_t, free);
    FIELD(device_plugin_t, memcpy_sync);
    FIELD(device_plugin_t, query_location);
    FIELD(device_plugin_t, get_device_count);
    FIELD(device_plugin_t, get_device_pci_bus_id);
    END();

#endif
    printf("handles\tengine=%zu\tbatch=%zu\tsegment=%zu\tlocal=%llu\n",
           sizeof(tent_engine_t), sizeof(tent_batch_id_t),
           sizeof(tent_segment_id_t), (unsigned long long)LOCAL_SEGMENT_ID);
    printf("opcodes\tread=%d\twrite=%d\n", OPCODE_READ, OPCODE_WRITE);
    printf(
        "status\twaiting=%d\tpending=%d\tinvalid=%d\tcanceled=%d\tcompleted=%"
        "d\ttimeout=%d\tfailed=%d\n",
        STATUS_WAITING, STATUS_PENDING, STATUS_INVALID, STATUS_CANCELED,
        STATUS_COMPLETED, STATUS_TIMEOUT, STATUS_FAILED);
    printf("permissions\tlocal_rw=%d\tglobal_ro=%d\tglobal_rw=%d\n",
           PERM_LOCAL_READ_WRITE, PERM_GLOBAL_READ_ONLY,
           PERM_GLOBAL_READ_WRITE);
    printf("segment_type\tmemory=%d\tfile=%d\n", TYPE_MEMORY, TYPE_FILE);
#ifndef ORBITE_C_API_ONLY
    printf("plugin\tsymbol=%s\thandled_error=%d\tlocation_bytes=%d\n",
           TENT_DEVICE_PLUGIN_REGISTER_SYMBOL, TENT_DEVICE_PLUGIN_ERROR,
           LOCATION_LEN);
#endif
    return 0;
}
