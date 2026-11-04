// Installed classic scatter requester for the packaged Rust hardware_peer source.
#include <transfer_engine.h>
#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <iostream>

static unsigned char pattern(uint64_t seed, size_t index) {
    uint64_t x = std::rotl(index * UINT64_C(0x9e3779b97f4a7c15) + seed, int(index % 63));
    return static_cast<uint8_t>(x ^ (x >> 17) ^ (x >> 41));
}
int main(int argc, char **argv) {
    assert(argc == 3);
    std::string name = argv[1], kind = argv[2];
    assert(kind == "cpu" || kind == "cuda");
    constexpr size_t guard = 64, payload = 4096, quarter = payload / 4;
    constexpr uint64_t seed = 37;
    mooncake::TransferEngine e;
    assert(e.init(P2PHANDSHAKE, "") == 0);
    auto peer = e.openSegment(name);
    std::vector<mooncake::SegmentBufferInfo> ranges;
    assert(!e.getSegmentBuffers(peer, ranges) && ranges.size() == 1);
    auto remote = ranges[0];
    assert(remote.length == payload + 2 * guard);
    assert((remote.location.starts_with("cuda:")) == (kind == "cuda"));
    std::vector<unsigned char> local(remote.length, 0xcc), expected = local;
    assert(!e.registerLocalMemory(local.data(), local.size()));
    std::array<size_t, 3> l{}, r{}, length{};
    for (size_t i = 0; i < 3; ++i) {
        l[i] = guard + i * quarter + 5;
        r[i] = guard + i * quarter + 11;
        length[i] = quarter - 23;
    }
    for (bool write : {false, true}) {
        std::array<unsigned, 3> receipts{};
        for (size_t i = 0; i < 3; ++i)
            for (size_t j = 0; j < length[i]; ++j) {
                const auto value =
                    write ? pattern(seed ^ 0xa55a5aa5, l[i] + j) : pattern(seed, r[i] + j - guard);
                if (write)
                    local[l[i] + j] = value;
                expected[l[i] + j] = value;
            }
        mooncake::TransferEngine::ScatterTransferRange op{write ? mooncake::TransferRequest::WRITE
                                                                : mooncake::TransferRequest::READ,
                                                          name,
                                                          remote.addr,
                                                          remote.length,
                                                          local.data(),
                                                          local.size(),
                                                          l,
                                                          r,
                                                          length,
                                                          [&](size_t i, const mooncake::Status &s) {
                                                              assert(s.ok() && i < receipts.size());
                                                              ++receipts[i];
                                                          },
                                                          0};
        auto s = e.transferScatter({op});
        if (!s.ok()) {
            std::cerr << s.ToString() << '\n';
            return 1;
        }
        assert(local == expected &&
               std::all_of(receipts.begin(), receipts.end(), [](auto n) { return n == 1; }));
        assert(!e.sendNotifyByID(peer, {write ? "write_done" : "read_done", std::to_string(seed)}));
    }
    std::vector<mooncake::NicLoadStats> nics;
    assert(e.getNicLoadStats(nics).ok() && !nics.empty());
    for (auto &nic : nics)
        std::cout << nic.device_name << " " << nic.inflight_bytes << " " << nic.ewma_bandwidth_bps
                  << '\n';
    assert(!e.unregisterLocalMemory(local.data()) && !e.closeSegment(peer));
    std::cout << "verified scatter bytes and guards\n";
}
