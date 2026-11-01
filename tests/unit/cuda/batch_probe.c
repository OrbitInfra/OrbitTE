/* Optional driver ABI probe, no CUDA library or hardware required. */
#include <stddef.h>
#include <stdint.h>
static int version = 13000;
void set_version(int value) { version = value; }
int cuDriverGetVersion(int *out) { *out = version; return version < 0 ? 1 : 0; }
/* The legacy ABI must never be selected as CUDA 13's no-failIdx ABI. */
int cuMemcpyBatchAsync(void) { return 700; }
#ifndef OLD_ONLY
typedef struct { int type, id; } location;
typedef struct { unsigned order; location source, destination; unsigned flags; } attributes;
int cuMemcpyBatchAsync_v2(uint64_t *dsts, uint64_t *srcs, size_t *sizes, size_t count,
                         attributes *attrs, size_t *indices, size_t num_attrs, void *stream) {
    return dsts[0] == 100 && srcs[0] == 200 && sizes[0] == 17 && count == 1 &&
        attrs[0].order == 1 && attrs[0].flags == 0 && attrs[0].source.type == 0 &&
        attrs[0].destination.type == 0 && indices[0] == 0 && num_attrs == 1 &&
        stream == (void *)3 ? 0 : 1;
}
#endif
