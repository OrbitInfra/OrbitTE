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
#include "common/base/status.h"
#include "error.h"
#include "transfer_metadata.h"
#include <vector>
namespace mooncake {
class Transport {
  public:
    using SegmentID = uint64_t;
    using SegmentHandle = SegmentID;
    using BatchID = uint64_t;
    using NotifyDesc = TransferMetadata::NotifyDesc;
    struct TransferRequest {
        enum OpCode { READ, WRITE };
        enum Priority { PRIO_HIGH = 0, PRIO_MEDIUM = 1, PRIO_LOW = 2 };

        static constexpr uint64_t kNoTaskGroup = 0;

        OpCode opcode;
        void *source;
        SegmentID target_id;
        uint64_t target_offset;
        size_t length;
        int advise_retry_cnt = 0;
        // Per-request transport pin, TENT only.
        int transport_hint = 0;
        // Business intent for TENT scheduling (0 = unspecified).
        int intent_type = 0;
        // Adjacent requests in a group may be scheduled as one unit.
        uint64_t task_group_id = kNoTaskGroup;
        // TENT transport selection priority; ignored by classic TE.
        int priority = PRIO_MEDIUM;
        // Optional local RNIC index for the classic RDMA transport. The index
        // refers to Topology::getHcaList(); -1 keeps normal topology-based
        // selection. The hint is advisory and only affects initial submission.
        int nic_hint = -1;
    };

    enum TransferStatusEnum { WAITING, PENDING, INVALID, CANCELED, COMPLETED, TIMEOUT, FAILED };

    struct TransferStatus {
        TransferStatusEnum s;
        size_t transferred_bytes;
    };

    struct NicLoadStats {
        std::string device_name;
        uint64_t inflight_bytes{0};
        double ewma_bandwidth_bps{0.0};
    };

    struct BufferEntry {
        void *addr;
        size_t length;
    };
    struct TransferTask;
    virtual ~Transport() = default;
    virtual const char *getName() const { return "tent_compat"; }
    virtual BatchID allocateBatchID(size_t) { return UINT64_MAX; }
    virtual Status freeBatchID(BatchID) { return Status::NotImplemented("use TransferEngine"); }
    virtual Status submitTransfer(BatchID, const std::vector<TransferRequest> &) {
        return Status::NotImplemented("use TransferEngine");
    }
    virtual Status submitTransferTask(const std::vector<TransferTask *> &) {
        return Status::NotImplemented("use TransferEngine");
    }
    virtual Status getTransferStatus(BatchID, size_t, TransferStatus &) {
        return Status::NotImplemented("use TransferEngine");
    }
    virtual int registerLocalMemory(void *, size_t, const std::string &, bool, bool) {
        return ERR_NOT_IMPLEMENTED;
    }
    virtual int unregisterLocalMemory(void *, bool) { return ERR_NOT_IMPLEMENTED; }
    virtual int registerLocalMemoryBatch(const std::vector<BufferEntry> &, const std::string &) {
        return ERR_NOT_IMPLEMENTED;
    }
    virtual int unregisterLocalMemoryBatch(const std::vector<void *> &) {
        return ERR_NOT_IMPLEMENTED;
    }
};
} // namespace mooncake
