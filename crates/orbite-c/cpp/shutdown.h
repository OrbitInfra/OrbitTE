#pragma once
#include "tent/transfer_engine.h"
namespace mooncake::detail {
void on_shutdown(const std::shared_ptr<tent::TransferEngine>& owner, tent_engine_t handle);
}
