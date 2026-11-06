// The classic API always delegates to the TENT source adapter.
#include "transfer_engine.h"
#include "tent/common/config.h"
#include "tent/runtime/topology.h"
#include "tent/cpp_v1.h"
#include "shutdown.h"
#include <algorithm>
#include <stdexcept>
namespace mooncake {
namespace {
int classic(tent::Status::Code code) {
    using C = tent::Status::Code;
    switch (code) {
    case C::kOk:
        return 0;
    case C::kInvalidArgument:
    case C::kInvalidEntry:
        return ERR_INVALID_ARGUMENT;
    case C::kTooManyRequests:
        return ERR_TOO_MANY_REQUESTS;
    case C::kAddressNotRegistered:
        return ERR_ADDRESS_NOT_REGISTERED;
    case C::kDeviceNotFound:
        return ERR_DEVICE_NOT_FOUND;
    case C::kInvalidMetadataType:
    case C::kNeedsRefreshCache:
    case C::kMetadataError:
        return ERR_METADATA;
    case C::kRpcServiceError:
    case C::kRpcConnectionError:
    case C::kRpcUnknownEffect:
        return ERR_SOCKET;
    case C::kMalformedJson:
        return ERR_MALFORMED_JSON;
    case C::kNotImplemented:
        return ERR_NOT_IMPLEMENTED;
    default:
        return ERR_CONTEXT;
    }
}
Status checked(const tent::Status &s) {
    return s.ok() ? Status::OK() : Status::Context(s.ToString());
}
Status closed() { return Status::Context("engine is unavailable"); }
} // namespace
TransferEngine::TransferEngine(bool) {}
TransferEngine::TransferEngine(bool, const std::vector<std::string> &filter)
    : tent_device_filter_(filter) {}
TransferEngine::TransferEngine(TransferEngine &&other) noexcept { *this = std::move(other); }
TransferEngine &TransferEngine::operator=(TransferEngine &&other) noexcept {
    if (this != &other) {
        std::scoped_lock lock(mutex_, other.mutex_);
        shutdown_requested_ = std::exchange(other.shutdown_requested_, false);
        impl_tent_ = std::move(other.impl_tent_);
        metadata_conn_string_ = std::move(other.metadata_conn_string_);
        tent_device_filter_ = std::move(other.tent_device_filter_);
        compatibility_ = std::move(other.compatibility_);
    }
    return *this;
}
TransferEngine::~TransferEngine() { freeEngine(); }
int TransferEngine::init(const std::string &metadata, const std::string &local,
                         const std::string &ip, uint64_t port) {
    return init(metadata, local, ip, port, "");
}
int TransferEngine::init(const std::string &metadata, const std::string &local, const std::string &,
                         uint64_t, const std::string &protocol) {
    std::lock_guard lock(mutex_);
    auto config = std::make_shared<tent::Config>();
    if (protocol == "tcp") {
        config->set("transports/force_tcp", true);
        config->set("transports/tcp/enable", true);
        config->set("transports/rdma/enable", false);
        config->set("transports/hp_tcp/enable", false);
        config->set("transports/fabric/enable", false);
    }
    if (!local.empty())
        config->set("local_segment_name", local);
    if (metadata == P2PHANDSHAKE)
        config->set("metadata_type", "p2p");
    else {
        auto sep = metadata.find("://");
        auto type = sep == std::string::npos ? "etcd" : metadata.substr(0, sep);
        auto servers = sep == std::string::npos ? metadata : metadata.substr(sep + 3);
        if (!type.empty())
            config->set("metadata_type", type);
        if (!servers.empty())
            config->set("metadata_servers", servers);
    }
    if (!tent_device_filter_.empty())
        config->set("topology/rdma_whitelist", tent_device_filter_);
    auto next = std::make_shared<tent::TransferEngine>(config);
    if (!next->available())
        return ERR_CONTEXT;
    if (shutdown_requested_)
        detail::on_shutdown(next, next->owner_);
    impl_tent_ = std::move(next);
    metadata_conn_string_ = metadata;
    return 0;
}
void TransferEngine::enableGracefulShutdown() {
    std::lock_guard lock(mutex_);
    detail::on_shutdown(impl_tent_, impl_tent_ ? impl_tent_->owner_ : nullptr);
    shutdown_requested_ = true;
}
int TransferEngine::freeEngine() {
    std::shared_ptr<tent::TransferEngine> retired;
    {
        std::lock_guard lock(mutex_);
        retired = std::move(impl_tent_);
        metadata_conn_string_.clear();
    }
    return 0;
}
Transport *TransferEngine::installTransport(const std::string &, void **) {
    std::lock_guard lock(mutex_);
    if (!compatibility_)
        compatibility_ = std::make_unique<Transport>();
    return compatibility_.get();
}
int TransferEngine::uninstallTransport(const std::string &) { return 0; }
Transport *TransferEngine::getTransport(const std::string &) { return nullptr; }
std::string TransferEngine::getLocalIpAndPort() {
    auto e = getTentEngine();
    return e ? e->getSegmentName() : "";
}
int TransferEngine::getRpcPort() {
    auto e = getTentEngine();
    return e ? e->getRpcServerPort() : 0;
}
SegmentHandle TransferEngine::openSegment(const std::string &name) {
    tent::SegmentID id;
    auto e = getTentEngine();
    return e && e->openSegment(id, name).ok() ? id
                                              : static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT);
}
int TransferEngine::getSegmentBuffers(SegmentHandle id, std::vector<SegmentBufferInfo> &out) {
    out.clear();
    auto e = getTentEngine();
    tent::SegmentInfo info;
    if (!e || !e->getSegmentInfo(id, info).ok())
        return ERR_METADATA;
    if (info.type != tent::SegmentInfo::Memory)
        return ERR_NOT_IMPLEMENTED;
    out.reserve(info.buffers.size());
    for (auto &b : info.buffers)
        out.push_back({b.base, b.length, std::move(b.location)});
    return 0;
}
Status TransferEngine::CheckSegmentStatus(SegmentID id) {
    auto e = getTentEngine();
    if (!e)
        return closed();
    auto s = e->probePeerAliveByID(id);
    return s.ok() ? Status::OK() : Status::Socket(s.ToString());
}
int TransferEngine::closeSegment(SegmentHandle id) {
    auto e = getTentEngine();
    return e ? classic(e->closeSegment(id).code()) : ERR_CONTEXT;
}
int TransferEngine::removeLocalSegment(const std::string &) { return 0; }
int TransferEngine::registerLocalMemory(void *p, size_t n, const std::string &location, bool remote,
                                        bool) {
    auto e = getTentEngine();
    if (!e)
        return ERR_CONTEXT;
    tent::MemoryOptions o;
    o.location = location.empty() ? "*" : location;
    o.perm = remote ? tent::kGlobalReadWrite : tent::kLocalReadWrite;
    return classic(e->registerLocalMemory(p, n, o).code());
}
int TransferEngine::unregisterLocalMemory(void *p, bool) {
    auto e = getTentEngine();
    return e ? classic(e->unregisterLocalMemory(p).code()) : ERR_CONTEXT;
}
int TransferEngine::registerLocalMemoryBatch(const std::vector<BufferEntry> &list,
                                             const std::string &location) {
    auto e = getTentEngine();
    if (!e)
        return ERR_CONTEXT;
    size_t limit = 0;
    if (orbite_limits_v1(e->owner_, &limit) || list.size() > limit)
        return ERR_TOO_MANY_REQUESTS;
    std::vector<void *> addr;
    std::vector<size_t> sizes;
    for (const auto &b : list) {
        addr.push_back(b.addr);
        sizes.push_back(b.length);
    }
    tent::MemoryOptions o;
    if (!location.empty())
        o.location = location;
    return classic(e->registerLocalMemory(std::move(addr), std::move(sizes), o).code());
}
int TransferEngine::unregisterLocalMemoryBatch(const std::vector<void *> &list) {
    auto e = getTentEngine();
    return e ? classic(e->unregisterLocalMemory(list).code()) : ERR_CONTEXT;
}
void *TransferEngine::allocateSharedMemory(size_t) { return nullptr; }
void *TransferEngine::allocateSharedMemory(size_t, const SharedMemoryOptions &) { return nullptr; }
int TransferEngine::freeSharedMemory(void *) { return ERR_NOT_IMPLEMENTED; }
BatchID TransferEngine::allocateBatchID(size_t n) {
    auto e = getTentEngine();
    const auto b = e ? e->allocateBatch(n) : 0;
    return b ? b : INVALID_BATCH_ID;
}
Status TransferEngine::freeBatchID(BatchID b) {
    auto e = getTentEngine();
    return e ? checked(e->freeBatch(b)) : closed();
}

Status TransferEngine::submit(const std::shared_ptr<tent::TransferEngine> &e, BatchID b,
                              const std::vector<TransferRequest> &list, const tent::Notification *n,
                              bool scatter) {
    if (!e)
        return closed();
    size_t limit = 0;
    if (orbite_limits_v1(e->owner_, &limit) || list.size() > limit)
        return Status::TooManyRequests("request limit");
    std::vector<tent::Request> requests;
    requests.reserve(list.size());
    for (const auto &r : list) {
        if ((r.opcode != TransferRequest::READ && r.opcode != TransferRequest::WRITE) ||
            r.intent_type < 0 || r.intent_type > 6 || r.transport_hint < 0 || r.transport_hint > 18)
            return Status::InvalidArgument("invalid transfer request");
        tent::Request req{};
        req.opcode = static_cast<tent::Request::OpCode>(r.opcode);
        req.source = r.source;
        req.target_id = r.target_id;
        req.target_offset = r.target_offset;
        req.length = r.length;
        req.priority = r.priority;
        req.intent_type = static_cast<tent::IntentType>(r.intent_type);
        req.transport_hint = static_cast<tent::TransportType>(r.transport_hint);
        requests.push_back(req);
    }
    auto s = e->submit(b, requests, n, scatter);
    return scatter && s.IsNotImplemented() ? Status::NotImplemented(s.message()) : checked(s);
}
Status TransferEngine::submitTransfer(BatchID b, const std::vector<TransferRequest> &r) {
    return submit(getTentEngine(), b, r, nullptr, false);
}
Status TransferEngine::submitTransferWithNotify(BatchID b, const std::vector<TransferRequest> &r,
                                                TransferMetadata::NotifyDesc n) {
    tent::Notification note{std::move(n.name), std::move(n.notify_msg)};
    return submit(getTentEngine(), b, r, &note, false);
}
int TransferEngine::getNotifies(std::vector<TransferMetadata::NotifyDesc> &out) {
    auto e = getTentEngine();
    if (!e)
        return ERR_CONTEXT;
    std::vector<tent::Notification> notes;
    auto s = e->receiveNotification(notes);
    if (!s.ok())
        return classic(s.code());
    for (auto &n : notes)
        out.push_back({std::move(n.name), std::move(n.msg)});
    return 0;
}
int TransferEngine::sendNotifyByID(SegmentID id, TransferMetadata::NotifyDesc n) {
    auto e = getTentEngine();
    return e ? classic(e->sendNotification(id, {std::move(n.name), std::move(n.notify_msg)}).code())
             : ERR_CONTEXT;
}
int TransferEngine::sendNotifyByName(std::string name, TransferMetadata::NotifyDesc n) {
    auto e = getTentEngine();
    if (!e)
        return ERR_CONTEXT;
    tent::SegmentID id;
    auto s = e->openSegment(id, name);
    return classic(
        (s.ok() ? e->sendNotification(id, {std::move(n.name), std::move(n.notify_msg)}) : s)
            .code());
}
PeerLiveness TransferEngine::probePeerAliveByID(SegmentID id) {
    return CheckSegmentStatus(id).ok() ? PeerLiveness::Alive : PeerLiveness::Unreachable;
}
Status TransferEngine::getTransferStatus(BatchID b, size_t i, TransferStatus &out) {
    auto e = getTentEngine();
    if (!e)
        return closed();
    tent::TransferStatus s{};
    auto r = e->getTransferStatus(b, i, s);
    if (r.ok())
        out = {static_cast<TransferStatusEnum>(s.s), s.transferred_bytes};
    return checked(r);
}
Status TransferEngine::getBatchTransferStatus(BatchID b, TransferStatus &out) {
    auto e = getTentEngine();
    if (!e)
        return closed();
    tent::TransferStatus s{};
    auto r = e->getTransferStatus(b, s);
    if (r.ok())
        out = {static_cast<TransferStatusEnum>(s.s), s.transferred_bytes};
    return checked(r);
}
Status TransferEngine::getNicLoadStats(std::vector<NicLoadStats> &out) const {
    auto e = getTentEngine();
    if (!e)
        return closed();
    std::vector<tent::NicLoadStats> stats;
    auto s = e->getNicLoadStats(stats);
    if (!s.ok())
        return checked(s);
    out.clear();
    for (auto &n : stats)
        out.push_back({std::move(n.device_name), n.inflight_bytes, n.ewma_bandwidth_bps});
    return Status::OK();
}
bool TransferEngine::isTcpOnly() const { return false; }
int TransferEngine::syncSegmentCache(const std::string &) { return 0; }
std::shared_ptr<TransferMetadata> TransferEngine::getMetadata() { return nullptr; }
bool TransferEngine::checkOverlap(void *, uint64_t) { return false; }
void TransferEngine::setAutoDiscover(bool) {}
void TransferEngine::setAutoDiscover(const AutoDiscoverConfig &) {}
void TransferEngine::setWhitelistFilters(std::vector<std::string> &&f) {
    std::lock_guard lock(mutex_);
    if (!impl_tent_)
        tent_device_filter_ = std::move(f);
}
int TransferEngine::numContexts() const { return 1; }
void *TransferEngine::getBaseAddr() { return nullptr; }
std::string TransferEngine::showLinks(bool json) const {
    return json ? "{}" : "(TENT mode or not initialized)";
}
std::string TransferEngine::getLocalTopologyString() {
    auto e = getTentEngine();
    return e ? e->getLocalTopologyString() : "{}";
}
std::shared_ptr<Topology> TransferEngine::getLocalTopology() {
    auto out = std::make_shared<Topology>();
    auto e = getTentEngine();
    if (!e)
        return out;
    auto topology = e->getLocalTopology();
    for (auto &m : topology->mem_list_) {
        if (m.name.empty() || m.name == "*")
            continue;
        TopologyEntry entry{m.name, {}, {}};
        for (size_t r = 0; r < tent::Topology::DevicePriorityRanks; ++r)
            for (auto id : m.device_list[r]) {
                const auto *nic = topology->getNicEntry(id);
                if (!nic)
                    continue;
                (r ? entry.avail_hca : entry.preferred_hca).push_back(nic->name);
                if (std::find(out->hcas_.begin(), out->hcas_.end(), nic->name) == out->hcas_.end())
                    out->hcas_.push_back(nic->name);
            }
        out->matrix_.emplace(m.name, std::move(entry));
    }
    return out;
}
std::string Topology::toString() const {
    nlohmann::json root = nlohmann::json::object();
    for (const auto &[name, m] : matrix_)
        root[name] = {m.preferred_hca, m.avail_hca};
    return root.dump();
}

} // namespace mooncake
