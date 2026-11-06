/* OrbitInfra/Mooncake extensions outside the Mooncake v0.3.14 C ABI. */
#ifndef ORBITTE_TENT_EXTENSIONS_H
#define ORBITTE_TENT_EXTENSIONS_H
#include "tent/transfer_engine.h"
#ifdef __cplusplus
extern "C" {
#endif
#define TENT_ERROR_TOO_MANY_REQUESTS 2
#define TENT_ERROR_RPC_CONNECTION 134
#define TENT_ERROR_RPC_UNKNOWN_EFFECT 135
#define TENT_ALL_TASKS SIZE_MAX
/* Observe cached logical outcomes. Neither output is a drain or replay receipt. */
int tent_terminal_result(tent_engine_t engine, tent_batch_id_t batch_id,
                         size_t task_id, int* status_code, int* effect_known);
#ifdef __cplusplus
}
#endif
#endif
