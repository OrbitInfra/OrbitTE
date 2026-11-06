// OrbitKV 93541a21, crates/orbitkv-core/src/transfer/kernel.cu (Apache-2.0).
// Regenerate the embedded driver image with Clang's CUDA frontend without an SDK:
// clang++-19 -x cuda --cuda-device-only --cuda-gpu-arch=sm_52 -nocudainc -nocudalib -O3 -S kernel.cu -o kernel.ptx
#ifdef __clang__
#define __global__ __attribute__((global))
#include <__clang_cuda_builtin_vars.h>
using int4 = int __attribute__((ext_vector_type(4)));
#endif

extern "C" __global__ void orbite_batch_copy(const unsigned long long* __restrict__ desc,
                                                int n, int ctas) {
    const int tid = threadIdx.x;
    const long long work_count = (long long)n * ctas;
    for (long long work = blockIdx.x; work < work_count; work += gridDim.x) {
        const int i = work / ctas;
        const int shard = work % ctas;
        char* dst = (char*)desc[3ULL * i];
        const char* src = (const char*)desc[3ULL * i + 1];
        const unsigned long long size = desc[3ULL * i + 2];
        const unsigned long long start = (unsigned long long)shard * blockDim.x + tid;
        const unsigned long long stride = (unsigned long long)blockDim.x * ctas;
        if ((((unsigned long long)dst | (unsigned long long)src) & 15ULL) == 0) {
            int4* dst4 = (int4*)dst;
            const int4* src4 = (const int4*)src;
            const unsigned long long n4 = size / 16;
            for (unsigned long long j = start; j < n4; j += stride) {
                dst4[j] = src4[j];
            }
            for (unsigned long long j = n4 * 16 + start; j < size; j += stride) {
                dst[j] = src[j];
            }
        } else {
            for (unsigned long long j = start; j < size; j += stride) {
                dst[j] = src[j];
            }
        }
    }
}
