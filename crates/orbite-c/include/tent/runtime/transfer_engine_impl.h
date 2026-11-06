// Source compatibility for TENT callers using the progress and synchronous helpers.
#pragma once
#include "tent/transfer_engine.h"
#include <algorithm>
#include <chrono>
#include <thread>
namespace mooncake::tent {
inline std::chrono::microseconds nextPollDelay(uint64_t polls) {
    if (polls < 128)
        return std::chrono::microseconds::zero();
    return std::chrono::microseconds(
        std::min<uint64_t>(200, uint64_t{1} << std::min<uint64_t>(polls - 128, 8)));
}
inline void waitBeforeNextPoll(uint64_t polls) {
    const auto delay = nextPollDelay(polls);
    if (delay.count())
        std::this_thread::sleep_for(delay);
    else
        std::this_thread::yield();
}
class TransferEngineImpl final : private TransferEngine {
  public:
    TransferEngineImpl() = default;
    TransferEngineImpl(std::shared_ptr<Config> config, bool config_loaded = true)
        : TransferEngine(config_loaded ? std::move(config) : nullptr) {}
    using TransferEngine::allocateBatch;
    using TransferEngine::allocateLocalMemory;
    using TransferEngine::available;
    using TransferEngine::cancelTransfer;
    using TransferEngine::closeSegment;
    using TransferEngine::exportLocalSegment;
    using TransferEngine::freeBatch;
    using TransferEngine::freeLocalMemory;
    using TransferEngine::getLocalTopology;
    using TransferEngine::getLocalTopologyString;
    using TransferEngine::getNicLoadStats;
    using TransferEngine::getRpcServerAddress;
    using TransferEngine::getRpcServerPort;
    using TransferEngine::getSegmentInfo;
    using TransferEngine::getSegmentName;
    using TransferEngine::getTransferStatus;
    using TransferEngine::importRemoteSegment;
    using TransferEngine::openSegment;
    using TransferEngine::probePeerAliveByID;
    using TransferEngine::progressBatch;
    using TransferEngine::receiveNotification;
    using TransferEngine::registerLocalMemory;
    using TransferEngine::sendNotification;
    using TransferEngine::submitTransfer;
    using TransferEngine::transferSync;
    using TransferEngine::unregisterLocalMemory;
    using TransferEngine::waitTransferCompletion;
};
} // namespace mooncake::tent
