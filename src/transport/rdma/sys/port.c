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

#ifdef ORBITTE_GPU_KEYS
#include <errno.h>
#if __has_include(<infiniband/mlx5dv.h>)
#include <infiniband/mlx5dv.h>
int orbitte_mlx5_pdn(int (*init)(struct mlx5dv_obj *, uint64_t), void *pd, uint32_t *pdn) {
    struct mlx5dv_pd result = {0};
    struct mlx5dv_obj object = {.pd = {.in = pd, .out = &result}};
    int code = init(&object, MLX5DV_OBJ_PD);
    if (!code) *pdn = result.pdn;
    return code;
}
#else
int orbitte_mlx5_pdn(int (*init)(void *, uint64_t), void *pd, uint32_t *pdn) {
    (void)init; (void)pd; (void)pdn;
    return EOPNOTSUPP;
}
#endif
#endif
