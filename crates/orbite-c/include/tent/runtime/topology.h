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

// NOTE: Guard renamed from TOPOLOGY_H to avoid collision with the classic
// transfer-engine's mooncake-transfer-engine/include/topology.h, which also
// uses TOPOLOGY_H.
#pragma once
#include "tent/common/types.h"
#include "tent/common/status.h"
namespace mooncake {
namespace tent {
class Platform;
class Topology {
   public:
    inline static constexpr size_t DevicePriorityRanks = 3;

    // Keep the existing RDMA/TCP numeric values stable for serialized
    // topologies. UB is a distinct link type and must never be selected as an
    // RDMA verbs device.
    enum NicType {
        NIC_RDMA = 0,
        NIC_TCP = 1,
        NIC_UNKNOWN = 2,
        NIC_UB = 3,
    };
    enum MemType {
        MEM_HOST,
        MEM_CUDA,
        MEM_ROCM,
        MEM_ASCEND,
        MEM_UNKNOWN,
        MEM_MLU
    };

    using NicID = int;
    struct NicEntry {
        std::string name;
        std::string pci_bus_id;
        NicType type{NIC_UNKNOWN};
        int numa_node{-1};

        // Hardware-specific discovery metadata is opaque to the common
        // topology layer. Transports own namespaced keys and interpretation.
        std::unordered_map<std::string, std::string> device_attrs{};
    };

    using MemID = int;
    struct MemEntry {
        std::string name;
        std::string pci_bus_id;
        MemType type;
        int numa_node;
        std::vector<NicID> device_list[DevicePriorityRanks];
    };

   public:
    Topology();

    ~Topology();

    bool empty() const;

    void clear();

    Status parse(const std::string& json_content);
    std::string toString() const;



    size_t getNicCount(NicType type = NIC_UNKNOWN) const;

    size_t getMemCount(MemType type = MEM_UNKNOWN) const;

    const NicEntry* getNicEntry(NicID id) const;

    const MemEntry* getMemEntry(MemID id) const;

    const NicEntry* getNicEntry(const std::string& name) const;

    const MemEntry* getMemEntry(const std::string& name) const;

    // True only when both NUMA ids are known and differ. Unknown (-1) is not
    // treated as remote; rank is ignored because probes disagree on placement.
    bool isCrossNuma(const MemEntry& mem, NicID nic_id) const;

    NicID getNicId(const std::string& name) const;

    MemID getMemId(const std::string& name) const;

    std::string getNicName(NicID id) const;

    NicType getNicType(NicID id) const;

    const std::string findNearMem(const std::string& name,
                                  MemType type = MEM_HOST) const;

   public:
    std::vector<NicEntry> nic_list_;
    std::vector<MemEntry> mem_list_;
};

 } // namespace tent
} // namespace mooncake
