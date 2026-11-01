#include "port.h"

#include <infiniband/verbs.h>

int orbitte_query_port(void *context, uint8_t port,
                      struct orbitte_port_attr *result) {
    struct ibv_port_attr attr = {0};
    int code = ibv_query_port((struct ibv_context *)context, port, &attr);
    if (code != 0)
        return code;

    uint32_t speed = attr.active_speed;
#ifdef ORBITTE_HAVE_ACTIVE_SPEED_EX
    if (attr.active_speed_ex != 0)
        speed = attr.active_speed_ex;
#endif
    *result = (struct orbitte_port_attr){
        .state = attr.state,
        .active_mtu = attr.active_mtu,
        .gid_tbl_len = attr.gid_tbl_len,
        .active_speed = speed,
        .lid = attr.lid,
        .link_layer = attr.link_layer,
        .active_width = attr.active_width,
    };
    return 0;
}
