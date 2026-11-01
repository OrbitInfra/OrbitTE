#ifndef ORBITTE_RDMA_PORT_H
#define ORBITTE_RDMA_PORT_H

#include <stdint.h>

/* Only these values cross the shim boundary; provider structs stay in C. */
struct orbitte_port_attr {
    uint32_t state;
    uint32_t active_mtu;
    int32_t gid_tbl_len;
    uint32_t active_speed;
    uint16_t lid;
    uint8_t link_layer;
    uint8_t active_width;
};

int orbitte_query_port(void *context, uint8_t port,
                      struct orbitte_port_attr *result);

#endif
