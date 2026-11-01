/* Public-header provider control; never opens hardware. */
#include <infiniband/verbs.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>

static int query(struct ibv_context *context, uint8_t port,
                 struct ibv_port_attr *attr, size_t length) {
    if (port != 7 || length != sizeof(*attr) || attr->active_speed != 0)
        return EINVAL;
#ifdef ORBITTE_HAVE_ACTIVE_SPEED_EX
    if (attr->active_speed_ex != 0)
        return EINVAL;
    if (context->num_comp_vectors == 0)
        attr->active_speed_ex = 256;
#endif
    attr->active_speed = 128;
    attr->active_width = 2;
    attr->state = IBV_PORT_ACTIVE;
    attr->active_mtu = IBV_MTU_4096;
    attr->lid = 42;
    attr->link_layer = IBV_LINK_LAYER_ETHERNET;
    attr->gid_tbl_len = 17;
    return context->num_comp_vectors == 2 ? EIO : 0;
}

void *orbitte_test_context(int mode) {
    static struct verbs_context context;
    memset(&context, 0, sizeof(context));
    context.sz = sizeof(context);
    context.context.abi_compat = __VERBS_ABI_IS_EXTENDED;
    context.context.num_comp_vectors = mode;
    context.query_port = query;
    return &context.context;
}

#ifdef ORBITTE_WITH_PORT_SPEED
int ibv_query_port_speed(struct ibv_context *context, uint32_t port, uint64_t *speed) {
    (void)context;
    *speed = UINT64_MAX;
    if (port == 2) return EIO;
    if (port == 1) *speed = 2000;
    if (port == 4) *speed = 0;
    return 0;
}
#endif
