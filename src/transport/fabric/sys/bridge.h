#ifndef ORBITE_FABRIC_BRIDGE_H
#define ORBITE_FABRIC_BRIDGE_H
#include <stddef.h>
#include <stdint.h>
/* No SDK object crosses the Rust ABI by value. No libfabric symbol is linked. */
struct orbite_fi_api {
    void *getinfo, *freeinfo, *dupinfo, *fabric, *version;
};
struct orbite_fi_info {
    char name[256];
    char provider[256];
    uint64_t mr_mode, max_message;
    size_t tx_size;
    int ipv4, virt_addr, endpoint_mr, provider_key, manual_progress, delivery_complete, numa;
};
struct orbite_fi_handles {
    void *fabric, *domain, *av, *cq, *ep;
};
struct orbite_fi_completion {
    void *context;
    size_t length;
    int error, provider_error;
};
int orbite_fi_discover(const struct orbite_fi_api *, int, const char *, void **);
void *orbite_fi_next(void *);
void orbite_fi_freeinfo(const struct orbite_fi_api *, void *);
int orbite_fi_describe(void *, struct orbite_fi_info *);
int orbite_fi_open(const struct orbite_fi_api *, void *, size_t, size_t, int,
                   struct orbite_fi_handles *, uint8_t *, size_t *);
int orbite_fi_close(void *);
int orbite_fi_register(const struct orbite_fi_handles *, void *, size_t, int,
                       uint64_t, int, void **, void **, uint64_t *);
int orbite_fi_insert(const struct orbite_fi_handles *, const uint8_t *, uint64_t *);
int orbite_fi_remove(const struct orbite_fi_handles *, uint64_t);
int orbite_fi_post(const struct orbite_fi_handles *, int, void *, size_t,
                   void *, uint64_t, uint64_t, uint64_t, void *);
int orbite_fi_poll(const struct orbite_fi_handles *, struct orbite_fi_completion *);
#endif
