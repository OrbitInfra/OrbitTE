// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include "tent/transfer_engine.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"

namespace mooncake {
class TransferEngine;
class TransferEngineImplTestPeer;
namespace tent {
class TransferEngineImpl;
class Config;
class Topology;
class TransferEngine {
   public:
    TransferEngine();

    TransferEngine(const std::string config_path);

    TransferEngine(std::shared_ptr<Config> config);

    ~TransferEngine();

    TransferEngine(const TransferEngine&) = delete;

    TransferEngine& operator=(const TransferEngine&) = delete;

   public:
    bool available() const;

    const std::string getSegmentName() const;

    const std::string getRpcServerAddress() const;

    uint16_t getRpcServerPort() const;

    // Returns the live local topology (nics/mems). Empty if engine unavailable.
    std::shared_ptr<Topology> getLocalTopology() const;

    // Native {"nics","mems"} JSON dump (includes rank0/1/2). "{}" if empty.
    std::string getLocalTopologyString() const;

   public:
    Status exportLocalSegment(std::string& shared_handle);

    Status importRemoteSegment(SegmentID& handle,
                               const std::string& shared_handle);

    Status openSegment(SegmentID& handle, const std::string& segment_name);

    Status closeSegment(SegmentID handle);

    Status getSegmentInfo(SegmentID handle, SegmentInfo& info);

   public:
    Status allocateLocalMemory(void** addr, size_t size,
                               Location location = kWildcardLocation);

    Status freeLocalMemory(void* addr);

    Status registerLocalMemory(void* addr, size_t size,
                               Permission permission = kGlobalReadWrite);

    Status unregisterLocalMemory(void* addr, size_t size = 0);

    Status registerLocalMemory(std::vector<void*> addr_list,
                               std::vector<size_t> size_list,
                               Permission permission = kGlobalReadWrite);

    Status unregisterLocalMemory(std::vector<void*> addr_list,
                                 std::vector<size_t> size_list = {});

    // advanced buffer allocate function
    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions& options);

    // advanced buffer register function
    Status registerLocalMemory(void* addr, size_t size, MemoryOptions& options);

    Status registerLocalMemory(std::vector<void*> addr_list,
                               std::vector<size_t> size_list,
                               MemoryOptions& options);

   public:
    BatchID allocateBatch(size_t batch_size);

    Status freeBatch(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request>& request_list);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request>& request_list,
                          const Notification& notifi);

    // Best-effort task cancellation. Work that has not reached the transport
    // is prevented from being submitted. Device work already posted may still
    // complete, so callers must continue polling for a terminal status.
    Status cancelTransfer(BatchID batch_id, size_t task_id);

    Status sendNotification(SegmentID target_id, const Notification& notifi);

    Status receiveNotification(std::vector<Notification>& notifi_list);

    Status probePeerAliveByID(SegmentID target_id);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status);

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus>& status_list);

    Status getTransferStatus(BatchID batch_id, TransferStatus& overall_status);

    // Drive one progress step on a batch and return its aggregated status.
    // Unlike getTransferStatus, this always allows internal failover/resubmit
    // regardless of enable_auto_failover_on_poll. The call is non-blocking and
    // performs at most one state-machine step per task; callers that want to
    // wait for completion must invoke it in a loop. PENDING means "make
    // progress later"; terminal states (COMPLETED/FAILED) will not be revived.
    Status progressBatch(BatchID batch_id, TransferStatus& overall_status);

    Status getNicLoadStats(std::vector<NicLoadStats>& stats) const;

    Status waitTransferCompletion(BatchID batch_id);
    Status transferSync(const std::vector<Request>& requests);
   private:
    Status submit(BatchID, const std::vector<Request>&, const Notification*, bool cancellable = false);
    tent_engine_t owner_{};
    friend class ::mooncake::TransferEngine;
};
}  // namespace tent
}  // namespace mooncake
