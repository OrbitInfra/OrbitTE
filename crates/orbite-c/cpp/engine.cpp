// Source adapter for the native TENT API. The Rust engine owns every transfer.
#include "tent/transfer_engine.h"
#include "tent/cpp_v1.h"
#include "tent/common/config.h"
#include "tent/runtime/topology.h"
#include "tent/runtime/transfer_engine_impl.h"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <thread>

namespace mooncake::tent {
namespace {
Status result(int rc, const char *operation) {
    return rc ? Status(static_cast<Status::Code>(-rc), operation) : Status::OK();
}
bool cstring(const std::string &value) { return value.find('\0') == std::string::npos; }
template <size_t N> bool copy_field(char (&out)[N], const std::string &s) {
    if (!cstring(s) || s.size() >= N)
        return false;
    std::memcpy(out, s.c_str(), s.size() + 1);
    return true;
}
Status options(const MemoryOptions &in, tent_memory_options_t &out) {
    if (!copy_field(out.location, in.location) || !copy_field(out.shm_path, in.shm_path))
        return Status::InvalidArgument("unrepresentable memory option");
    out.permission = in.perm;
    out.transport_type = in.type;
    out.shm_offset = in.shm_offset;
    out.internal = in.internal;
    return Status::OK();
}
orbite_notification_v1 view(const Notification &n) {
    return {n.name.data(), n.name.size(), n.msg.data(), n.msg.size()};
}
template <class C, class T, class Query, class Convert>
Status read_list(std::vector<T> &dest, Query query, Convert convert) {
    C scratch{};
    size_t count = 0;
    if (auto rc = query(&scratch, &count))
        return result(rc, "list size");
    std::vector<C> buf(count);
    if (auto rc = query(count ? buf.data() : &scratch, &count))
        return result(rc, "list snapshot");
    if (count > buf.size())
        return Status::TooManyRequests("list grew during query");
    std::vector<T> snapshot;
    snapshot.reserve(count);
    for (size_t i = 0; i < count; ++i)
        snapshot.push_back(convert(buf[i]));
    dest = std::move(snapshot);
    return Status::OK();
}
TransferStatus status(const tent_status_t &s) {
    return {static_cast<TransferStatusEnum>(s.status), s.transferred_bytes};
}
struct ConsumedBatch {
    tent_engine_t engine;
    BatchID id;
    ~ConsumedBatch() {
        if (id)
            (void)orbite_abandon_v1(engine, id);
    }
};

} // namespace

TransferEngine::TransferEngine() : owner_(orbite_create_v1(0, nullptr)) {}
TransferEngine::TransferEngine(const std::string path)
    : owner_(cstring(path) ? orbite_create_v1(2, path.c_str()) : nullptr) {}
TransferEngine::TransferEngine(std::shared_ptr<Config> config) {
    if (config) {
        auto json = config->dump(-1);
        owner_ = orbite_create_v1(1, json == "null" ? "{}" : json.c_str());
    }
}
TransferEngine::~TransferEngine() { tent_destroy_engine(owner_); }
bool TransferEngine::available() const { return tent_available(owner_) != 0; }
const std::string TransferEngine::getSegmentName() const {
    char name[256]{};
    return tent_segment_name(owner_, name, sizeof name) ? std::string{} : name;
}
const std::string TransferEngine::getRpcServerAddress() const {
    char addr[256]{};
    uint16_t port = 0;
    return tent_rpc_server_addr_port(owner_, addr, sizeof addr, &port) ? std::string{} : addr;
}
uint16_t TransferEngine::getRpcServerPort() const {
    char addr[256]{};
    uint16_t port = 0;
    return tent_rpc_server_addr_port(owner_, addr, sizeof addr, &port) ? 0 : port;
}
std::string TransferEngine::getLocalTopologyString() const {
    size_t n = 0;
    if (orbite_topology_v1(owner_, nullptr, &n) != -2 || !n || n > (1u << 20))
        return "{}";
    std::vector<char> json(n);
    if (orbite_topology_v1(owner_, json.data(), &n))
        return "{}";
    return std::string(json.data(), n - 1);
}
std::shared_ptr<Topology> TransferEngine::getLocalTopology() const {
    auto topology = std::make_shared<Topology>();
    topology->parse(getLocalTopologyString());
    return topology;
}
Status TransferEngine::exportLocalSegment(std::string &) {
    return Status::NotImplemented("exportLocalSegment");
}
Status TransferEngine::importRemoteSegment(SegmentID &, const std::string &) {
    return Status::NotImplemented("importRemoteSegment");
}
Status TransferEngine::openSegment(SegmentID &id, const std::string &name) {
    return cstring(name) ? result(tent_open_segment(owner_, &id, name.c_str()), "open segment")
                         : Status::InvalidArgument("embedded NUL in segment name");
}
Status TransferEngine::closeSegment(SegmentID id) {
    return result(tent_close_segment(owner_, id), "close segment");
}
Status TransferEngine::probePeerAliveByID(SegmentID id) {
    return result(orbite_probe_v1(owner_, id), "probe peer");
}
Status TransferEngine::getSegmentInfo(SegmentID id, SegmentInfo &out) {
    tent_segment_info_t info{};
    if (auto rc = tent_get_segment_info(owner_, id, &info))
        return result(rc, "segment info");
    std::unique_ptr<tent_segment_info_t, decltype(&tent_free_segment_info)> guard(
        &info, tent_free_segment_info);
    SegmentInfo snapshot{static_cast<SegmentInfo::Type>(info.type), {}};
    snapshot.buffers.reserve(info.num_buffers);
    for (int i = 0; i < info.num_buffers; ++i) {
        const auto &b = info.buffers[i];
        snapshot.buffers.push_back({b.base, b.length, b.location});
    }
    out = std::move(snapshot);
    return Status::OK();
}
Status TransferEngine::allocateLocalMemory(void **p, size_t n, Location loc) {
    return cstring(loc) ? result(tent_allocate_memory(owner_, p, n, loc.c_str()), "allocate")
                        : Status::InvalidArgument("embedded NUL in location");
}
Status TransferEngine::freeLocalMemory(void *p) {
    return result(tent_free_memory(owner_, p), "free memory");
}
Status TransferEngine::registerLocalMemory(void *p, size_t n, Permission perm) {
    return result(tent_register_memory_with_perm(owner_, p, n, perm), "register");
}
Status TransferEngine::unregisterLocalMemory(void *p, size_t n) {
    return result(tent_unregister_memory(owner_, p, n), "unregister");
}
Status TransferEngine::registerLocalMemory(std::vector<void *> p, std::vector<size_t> n,
                                           Permission perm) {
    if (p.size() != n.size())
        return Status::InvalidArgument("address and size counts differ");
    if (p.empty())
        return Status::OK();
    return result(tent_register_memory_batch(owner_, p.data(), n.data(), n.size(), perm),
                  "register ranges");
}
Status TransferEngine::unregisterLocalMemory(std::vector<void *> p, std::vector<size_t> n) {
    if (!n.empty() && p.size() != n.size())
        return Status::InvalidArgument("address and size counts differ");
    if (p.empty())
        return Status::OK();
    return result(
        tent_unregister_memory_batch(owner_, p.data(), n.empty() ? nullptr : n.data(), p.size()),
        "unregister ranges");
}
Status TransferEngine::allocateLocalMemory(void **p, size_t n, MemoryOptions &opt) {
    tent_memory_options_t o{};
    auto s = options(opt, o);
    if (!s.ok())
        return s;
    return result(tent_allocate_memory_ex(owner_, p, n, &o), "allocate");
}
Status TransferEngine::registerLocalMemory(void *p, size_t n, MemoryOptions &opt) {
    tent_memory_options_t o{};
    auto s = options(opt, o);
    if (!s.ok())
        return s;
    return result(tent_register_memory_ex(owner_, p, n, &o), "register");
}
Status TransferEngine::registerLocalMemory(std::vector<void *> p, std::vector<size_t> n,
                                           MemoryOptions &opt) {
    if (p.size() != n.size())
        return Status::InvalidArgument("address and size counts differ");
    tent_memory_options_t o{};
    auto s = options(opt, o);
    if (!s.ok() || p.empty())
        return s;
    return result(tent_register_memory_batch_ex(owner_, p.data(), n.data(), n.size(), &o),
                  "register ranges");
}
BatchID TransferEngine::allocateBatch(size_t n) {
    return owner_ ? tent_allocate_batch(owner_, n) : 0;
}
Status TransferEngine::freeBatch(BatchID b) {
    return result(tent_free_batch(owner_, b), "free batch");
}
Status TransferEngine::submit(BatchID b, const std::vector<Request> &list, const Notification *n,
                              bool cancellable) {
    size_t limit = 0;
    if (auto rc = orbite_limits_v1(owner_, &limit))
        return result(rc, "submit limits");
    if (list.size() > limit)
        return Status::TooManyRequests("request vector exceeds engine capacity");
    std::vector<orbite_request_v1> rows;
    rows.reserve(list.size());
    for (const auto &r : list)
        rows.push_back({sizeof(orbite_request_v1),
                        1,
                        {r.opcode, r.source, r.target_id, r.target_offset, r.length, r.priority,
                         r.transport_hint},
                        r.policy_name ? r.policy_name->data() : nullptr,
                        r.policy_name ? r.policy_name->size() : 0,
                        r.deadline_ns,
                        static_cast<int32_t>(r.intent_type),
                        0});
    orbite_notification_v1 notification{};
    if (n)
        notification = view(*n);
    return result(orbite_submit_v1(owner_, b, rows.data(), rows.size(),
                                    n ? &notification : nullptr, cancellable ? 1 : 0),
                  "submit transfer");
}
Status TransferEngine::submitTransfer(BatchID b, const std::vector<Request> &r) {
    return submit(b, r, nullptr);
}
Status TransferEngine::submitTransfer(BatchID b, const std::vector<Request> &r,
                                      const Notification &n) {
    return submit(b, r, &n);
}
Status TransferEngine::cancelTransfer(BatchID b, size_t i) {
    return result(tent_cancel_task(owner_, b, i), "cancel");
}
Status TransferEngine::getTransferStatus(BatchID b, size_t i, TransferStatus &s) {
    tent_status_t out{};
    int rc = tent_task_status(owner_, b, i, &out);
    if (!rc)
        s = status(out);
    return result(rc, "task status");
}
Status TransferEngine::getTransferStatus(BatchID b, std::vector<TransferStatus> &out) {
    return read_list<tent_status_t>(
        out, [&](auto *s, size_t *n) { return tent_task_status_list(owner_, b, s, n); }, status);
}
Status TransferEngine::getTransferStatus(BatchID b, TransferStatus &s) {
    tent_status_t out{};
    int rc = tent_overall_status(owner_, b, &out);
    if (!rc)
        s = status(out);
    return result(rc, "overall status");
}
Status TransferEngine::progressBatch(BatchID b, TransferStatus &s) {
    tent_status_t out{};
    int rc = orbite_progress_v1(owner_, b, &out);
    if (!rc)
        s = status(out);
    return result(rc, "progress batch");
}
Status TransferEngine::sendNotification(SegmentID id, const Notification &n) {
    auto note = view(n);
    return result(orbite_notify_v1(owner_, id, &note), "send notification");
}
Status TransferEngine::receiveNotification(std::vector<Notification> &out) {
    uint8_t *p = nullptr;
    size_t n = 0;
    if (auto rc = orbite_recv_v1(owner_, &p, &n))
        return result(rc, "receive notifications");
    std::unique_ptr<uint8_t, decltype(&std::free)> guard(p, std::free);
    std::vector<Notification> list;
    auto u32 = [](const uint8_t *v) {
        return uint32_t(v[0]) | (uint32_t(v[1]) << 8) | (uint32_t(v[2]) << 16) |
               (uint32_t(v[3]) << 24);
    };
    while (n) {
        if (n < 8)
            return Status::InternalError("truncated notification lengths");
        const size_t a = u32(p), b = u32(p + 4);
        p += 8;
        n -= 8;
        if (a > n || b > n - a)
            return Status::InternalError("truncated notification");
        list.push_back({std::string(reinterpret_cast<char *>(p), a),
                        std::string(reinterpret_cast<char *>(p + a), b)});
        p += a + b;
        n -= a + b;
    }
    out = std::move(list);
    return Status::OK();
}
Status TransferEngine::getNicLoadStats(std::vector<NicLoadStats> &out) const {
    return read_list<tent_nic_load_stat_t>(
        out, [&](auto *s, size_t *n) { return tent_get_nic_load_stats(owner_, s, n); },
        [](const auto &s) {
            return NicLoadStats{s.device_name, s.inflight_bytes, s.ewma_bandwidth_bps};
        });
}
Status TransferEngine::waitTransferCompletion(BatchID b) {
    ConsumedBatch owner{owner_, b};
    unsigned polls = 0;
    for (;;) {
        TransferStatus s{};
        auto op = progressBatch(b, s);
        if (!op.ok())
            return op;
        if (s.s != INITIAL && s.s != PENDING) {
            const int rc = orbite_reap_v1(owner_, b);
            if (rc != -TENT_ERROR_TOO_MANY_REQUESTS) {
                if (rc)
                    return result(rc, "batch retirement");
                owner.id = 0;
                return s.s == COMPLETED ? Status::OK() : Status::InternalError("transfer failed");
            }
        }
        waitBeforeNextPoll(polls);
        if (polls < 136)
            ++polls;
    }
}
Status TransferEngine::transferSync(const std::vector<Request> &requests) {
    if (requests.empty())
        return Status::OK();
    auto b = allocateBatch(requests.size());
    if (!b)
        return Status::TooManyRequests("allocate batch");
    ConsumedBatch owner{owner_, b};
    auto s = submitTransfer(b, requests);
    if (!s.ok())
        return s;
    owner.id = 0;
    return waitTransferCompletion(b);
}
} // namespace mooncake::tent
