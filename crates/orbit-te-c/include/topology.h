// Read-only classic rank projection of the installed TENT engine's topology.
#pragma once
#include <string>
#include <unordered_map>
#include <vector>
namespace mooncake {
class TransferEngine;
struct TopologyEntry {
    std::string name;
    std::vector<std::string> preferred_hca;
    std::vector<std::string> avail_hca;
    bool operator==(const TopologyEntry &) const = default;
};
using TopologyMatrix = std::unordered_map<std::string, TopologyEntry>;
class Topology {
  public:
    TopologyMatrix getMatrix() const { return matrix_; }
    const std::vector<std::string> &getHcaList() const { return hcas_; }
    std::string toString() const;
    bool empty() const { return matrix_.empty(); }

  private:
    friend class TransferEngine;
    TopologyMatrix matrix_;
    std::vector<std::string> hcas_;
};
} // namespace mooncake
