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
#include <string>
#include <cstddef>
#define P2PHANDSHAKE "P2PHANDSHAKE"
namespace mooncake {
inline const std::string kWildcardLocation = "*";
class TransferMetadata {
 public:
    struct NotifyDesc {std::string name, notify_msg;};
};
struct SharedMemoryOptions {
    bool use_hugepage = false;
    size_t hugepage_size = 0;    // 0 → 2MB when use_hugepage
    std::string hugetlbfs_path;  // empty → size-specific default mount
    bool populate = true;        // Store passes false and populates itself

    static constexpr size_t kHugepage2MB = 2ULL << 20;
    static constexpr size_t kHugepage512MB = 512ULL << 20;
    static constexpr size_t kHugepage1GB = 1ULL << 30;

    static bool isSupportedHugepageSize(size_t size) {
        return size == kHugepage2MB || size == kHugepage512MB ||
               size == kHugepage1GB;
    }

    static const char *defaultHugetlbfsPathFor(size_t hugepage_size) {
        if (hugepage_size == kHugepage1GB) return "/dev/hugepages-1G";
        if (hugepage_size == kHugepage512MB) return "/dev/hugepages-512M";
        return "/dev/hugepages";
    }
};
} // namespace mooncake
