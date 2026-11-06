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

#include "tent/common/config.h"



#include <algorithm>
#include <cctype>
#include <sstream>

namespace mooncake {
namespace tent {
namespace {

void collectConfigPaths(const json& node, const std::string& prefix,
                        std::vector<std::string>& paths) {
    // A default-constructed Config has a null root and represents an empty
    // configuration whose callers rely entirely on defaults.
    if (prefix.empty() && node.is_null()) return;
    if (!node.is_object()) {
        paths.push_back(prefix);
        return;
    }
    if (node.empty()) {
        if (!prefix.empty()) paths.push_back(prefix);
        return;
    }

    for (auto it = node.begin(); it != node.end(); ++it) {
        std::string path = prefix.empty() ? it.key() : prefix + "/" + it.key();
        collectConfigPaths(it.value(), path, paths);
    }
}

}  // namespace

Status Config::load(const std::string& content) {
    if (content.size() > (1u << 20)) return Status::TooManyRequests("configuration exceeds 1 MiB");
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        config_data_ = json::parse(content);
        return Status::OK();
    } catch (const json::parse_error& e) {
        // what() includes the last input token, which may contain credentials.
        return Status::InvalidArgument("Invalid JSON: parse error " +
                                       std::to_string(e.id) + " at byte " +
                                       std::to_string(e.byte) + LOC_MARK);
    } catch (const json::exception& e) {
        return Status::InvalidArgument("Invalid JSON: parse error " +
                                       std::to_string(e.id) + LOC_MARK);
    } catch (const std::exception&) {
        return Status::InvalidArgument("Invalid JSON: parsing failed" LOC_MARK);
    }
}

Status Config::loadFile(const std::string& file_path) {
    std::ifstream ifs(file_path);
    if (!ifs.is_open()) {
        return Status::InvalidArgument(
            std::string("Cannot open config file: ") + file_path + LOC_MARK);
    }
    std::string content;
    char chunk[4096];
    while (ifs.read(chunk, sizeof chunk) || ifs.gcount()) {
        if (content.size() + ifs.gcount() > (1u << 20)) return Status::TooManyRequests("configuration exceeds 1 MiB");
        content.append(chunk, ifs.gcount());
    }
    if (ifs.bad()) return Status::InvalidArgument("cannot read configuration");
    return load(content);
}

std::string Config::dump(int indent) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return config_data_.dump(indent);
}

bool Config::dumpSubtree(const std::string& key_path, std::string* out) const {
    if (!out || key_path.empty()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    const json* node = &config_data_;
    std::string::size_type start = 0;
    bool nested_found = true;
    while (start < key_path.size()) {
        auto pos = key_path.find(kDelimiter, start);
        auto segment = key_path.substr(start, pos - start);
        start = (pos == std::string::npos) ? key_path.size() : pos + 1;
        if (segment.empty()) continue;
        auto it = node->find(segment);
        if (it == node->end()) {
            nested_found = false;
            break;
        }
        node = &(*it);
    }
    if (nested_found && !node->is_null()) {
        *out = node->dump();
        return true;
    }
    auto flat_it = config_data_.find(key_path);
    if (flat_it != config_data_.end() && !flat_it->is_null()) {
        *out = flat_it->dump();
        return true;
    }
    return false;
}

std::shared_ptr<const Config> Config::freeze() const {
    auto frozen = std::make_shared<Config>();
    std::lock_guard<std::mutex> lock(mutex_);
    frozen->config_data_ = config_data_;
    return frozen;
}

std::vector<std::string> Config::paths() const {
    std::vector<std::string> paths;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        collectConfigPaths(config_data_, "", paths);
    }
    std::sort(paths.begin(), paths.end());
    paths.erase(std::unique(paths.begin(), paths.end()), paths.end());
    return paths;
}

}  // namespace tent
}  // namespace mooncake
