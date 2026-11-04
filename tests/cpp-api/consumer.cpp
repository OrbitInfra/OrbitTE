// Downstream C++20 client. All API and layout declarations come from the installation.
#include <transfer_engine.h>
#include <tent/transfer_engine.h>
#include <tent/cpp_v1.h>
#include <tent/common/config.h>
#include <tent/runtime/topology.h>
#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#include <type_traits>

static_assert(sizeof(orbitte_request_v1) == 88 && alignof(orbitte_request_v1) == 8);
static_assert(offsetof(orbitte_request_v1, request) == 8 &&
              offsetof(orbitte_request_v1, policy_name) == 56 &&
              offsetof(orbitte_request_v1, policy_name_len) == 64 &&
              offsetof(orbitte_request_v1, deadline_ns) == 72 &&
              offsetof(orbitte_request_v1, intent_type) == 80 &&
              offsetof(orbitte_request_v1, reserved) == 84);
static_assert(sizeof(orbitte_notification_v1) == 32 && sizeof(orbitte_task_v1) == 24);
namespace native = mooncake::tent;
namespace classic = mooncake;
using Bytes = std::array<unsigned char, 32>;
template <class S> void ok(const S &s) {
    if (!s.ok()) {
        std::cerr << s.ToString() << '\n';
        std::abort();
    }
}
static native::Request request(native::Request::OpCode op, void *local, uint64_t remote,
                               uint64_t addr, size_t n) {
    native::Request r{};
    r.opcode = op;
    r.source = local;
    r.target_id = remote;
    r.target_offset = addr;
    r.length = n;
    return r;
}
static std::shared_ptr<native::Config> config(const std::string &host = "127.0.0.1") {
    auto c = std::make_shared<native::Config>();
    c->set("rpc_server_hostname", host);
    c->set("rpc_server_port", 0);
    c->set("transports/rdma/enable", false);
    c->set("transports/nvlink/enable", false);
    c->set("metrics/enabled", false);
    return c;
}
static std::string message() { return std::string(4200, 'm') + std::string("\0tail", 5); }
static Bytes initial() {
    Bytes b;
    b.fill('!');
    std::memcpy(b.data() + 4, "source", 6);
    return b;
}

static void source(bool old) {
    classic::TransferEngine shim;
    std::shared_ptr<native::TransferEngine> e;
    if (old) {
        assert(shim.init(P2PHANDSHAKE, "") == 0);
        e = shim.getTentEngine();
    } else
        e = std::make_shared<native::TransferEngine>(config());
    assert(e && e->available());
    auto backing = initial();
    Bytes read_only{};
    read_only.fill('R');
    if (old)
        assert(shim.registerLocalMemory(backing.data(), backing.size()) == 0);
    else
        ok(e->registerLocalMemory(backing.data(), backing.size()));
    ok(e->registerLocalMemory(read_only.data(), 5, native::kGlobalReadOnly));
    std::cout << e->getSegmentName() << std::endl;
    std::string command;
    std::getline(std::cin, command);
    if (command == "verify") {
        auto expected = initial();
        std::memcpy(expected.data() + 13, "WRITE", 5);
        assert(backing == expected);
        std::vector<native::Notification> notes;
        ok(e->receiveNotification(notes));
        assert(notes.size() == 1 && notes[0].name == std::string(301, 'n') &&
               notes[0].msg == message());
    }
    if (command == "scatter") {
        auto expected = initial();
        std::memcpy(expected.data() + 13, "WRITE", 5);
        assert(backing == expected);
    }
    if (command == "untouched")
        assert(backing == initial());
    for (auto v : read_only)
        assert(v == 'R');
    ok(e->unregisterLocalMemory(read_only.data()));
    if (old)
        assert(shim.unregisterLocalMemory(backing.data()) == 0);
    else
        ok(e->unregisterLocalMemory(backing.data()));
}
static void native_client(const std::string &name) {
    native::TransferEngine e(config());
    assert(e.available());
    assert(e.getRpcServerAddress() == "127.0.0.1" && e.getRpcServerPort());
    native::SegmentID peer;
    ok(e.openSegment(peer, name));
    ok(e.probePeerAliveByID(peer));
    native::SegmentInfo info;
    ok(e.getSegmentInfo(peer, info));
    assert(info.buffers.size() == 2);
    auto dst = std::find_if(info.buffers.begin(), info.buffers.end(),
                            [](auto &b) { return b.length == 32; });
    assert(dst != info.buffers.end());
    Bytes local;
    local.fill('?');
    std::memcpy(local.data() + 20, "WRITE", 5);
    ok(e.registerLocalMemory(local.data(), local.size()));
    auto b = e.allocateBatch(2);
    assert(b);
    auto r = request(native::Request::READ, local.data() + 3, peer, dst->base + 4, 6);
    auto w = request(native::Request::WRITE, local.data() + 20, peer, dst->base + 13, 5);
    w.transport_hint = native::TCP;
    w.policy_name = "memory_default";
    ok(e.submitTransfer(b, {r, w}, {std::string(301, 'n'), message()}));
    native::TransferStatus s{};
    auto until = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    do {
        assert(std::chrono::steady_clock::now() < until);
        ok(e.progressBatch(b, s));
    } while (s.s == native::PENDING || s.s == native::INITIAL);
    assert(s.s == native::COMPLETED && s.transferred_bytes == 11);
    std::vector<native::TransferStatus> tasks;
    ok(e.getTransferStatus(b, tasks));
    assert(tasks.size() == 2 && tasks[0].transferred_bytes == 6);
    ok(e.getTransferStatus(b, 1, s));
    assert(s.s == native::COMPLETED && s.transferred_bytes == 5);
    ok(e.getTransferStatus(b, s));
    ok(e.cancelTransfer(b, 1));
    ok(e.waitTransferCompletion(b));
    assert(!e.getTransferStatus(b, s).ok());
    Bytes expected;
    expected.fill('?');
    std::memcpy(expected.data() + 3, "source", 6);
    std::memcpy(expected.data() + 20, "WRITE", 5);
    assert(local == expected);
    ok(e.transferSync({r}));
    assert(local == expected);
    ok(e.unregisterLocalMemory(local.data()));
    ok(e.closeSegment(peer));
    assert(!e.probePeerAliveByID(peer).ok());
}

static void classic_client(const std::string &name) {
    classic::TransferEngine e(false, {});
    assert(e.isUsingTent() && !e.getTentEngine());
    assert(!e.init(P2PHANDSHAKE, "", "", 12345, "tcp"));
    assert(e.getMetadataConnectionString() == P2PHANDSHAKE);
    auto p = e.openSegment(name);
    assert(p != UINT64_MAX);
    assert(e.probePeerAliveByID(p) == classic::PeerLiveness::Alive);
    ok(e.CheckSegmentStatus(p));
    std::vector<classic::SegmentBufferInfo> regions;
    assert(!e.getSegmentBuffers(p, regions) && regions.size() == 2);
    auto remote =
        std::find_if(regions.begin(), regions.end(), [](auto &b) { return b.length == 32; });
    assert(remote != regions.end());
    Bytes local;
    local.fill('?');
    std::memcpy(local.data() + 20, "WRITE", 5);
    assert(!e.registerLocalMemoryBatch({{local.data(), local.size()}}, "*"));
    auto b = e.allocateBatchID(2);
    assert(b != classic::INVALID_BATCH_ID);
    classic::TransferRequest r{}, w{};
    r.opcode = classic::TransferRequest::READ;
    r.source = local.data() + 3;
    r.target_id = p;
    r.target_offset = remote->addr + 4;
    r.length = 6;
    r.intent_type = 2;
    r.transport_hint = TRANSPORT_TCP;
    w.opcode = classic::TransferRequest::WRITE;
    w.source = local.data() + 20;
    w.target_id = p;
    w.target_offset = remote->addr + 13;
    w.length = 5;
    ok(e.submitTransferWithNotify(b, {r, w}, {std::string(301, 'n'), message()}));
    auto until = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    classic::TransferStatus s{};
    do {
        assert(std::chrono::steady_clock::now() < until);
        ok(e.getBatchTransferStatus(b, s));
    } while (s.s == classic::Transport::PENDING);
    assert(s.s == classic::Transport::COMPLETED && s.transferred_bytes == 11);
    ok(e.getTransferStatus(b, 0, s));
    assert(s.transferred_bytes == 6);
    ok(e.getTentEngine()->waitTransferCompletion(b));
    Bytes expected;
    expected.fill('?');
    std::memcpy(expected.data() + 3, "source", 6);
    std::memcpy(expected.data() + 20, "WRITE", 5);
    assert(local == expected);
    for (int intent : {-1, 7}) {
        auto bad = r;
        bad.intent_type = intent;
        b = e.allocateBatchID(1);
        assert(e.submitTransfer(b, {bad}).IsInvalidArgument());
        ok(e.freeBatchID(b));
    }
    auto compat = e.installTransport("tcp", nullptr);
    assert(compat && std::string(compat->getName()) == "tent_compat");
    assert(compat->submitTransfer(1, {r}).IsNotImplemented() &&
           compat->registerLocalMemory(nullptr, 0, "*", false, false) == ERR_NOT_IMPLEMENTED);
    assert(!e.getTransport("tcp") && !e.allocateSharedMemory(4096) &&
           e.freeSharedMemory(nullptr) == ERR_NOT_IMPLEMENTED);
    std::unordered_map<std::string, std::vector<classic::TransferEngine::RegisteredBuffer>> mp;
    std::string proto = "tcp";
    assert(e.mp_registerLocalMemory(mp) == ERR_NOT_IMPLEMENTED &&
           e.mp_unregisterLocalMemory(mp) == ERR_NOT_IMPLEMENTED);
    assert(e.mp_submitTransfer(b, {r}, proto).IsNotImplemented() &&
           e.mp_submitTransferWithNotify(b, {r}, {}, proto).IsNotImplemented());
    assert(!e.sendNotifyByName(e.getLocalIpAndPort(), {"self", "one"}));
    assert(!e.sendNotifyByID(LOCAL_SEGMENT_ID, {"self", "two"}));
    std::vector<classic::TransferMetadata::NotifyDesc> notes;
    assert(!e.getNotifies(notes) && notes.size() == 2 && notes[1].notify_msg == "two");
    auto topology = e.getLocalTopology();
    assert(topology);
    auto native_topology = e.getTentEngine()->getLocalTopology();
    for (const auto &mem : native_topology->mem_list_)
        if (mem.name != "*")
            assert(topology->getMatrix().contains(mem.name));
    std::vector<classic::NicLoadStats> nics;
    ok(e.getNicLoadStats(nics));
    assert(!e.unregisterLocalMemoryBatch({local.data()}));
    assert(!e.closeSegment(p));
    assert(!e.CheckSegmentStatus(p).ok());
    classic::TransferEngine moved(std::move(e));
    assert(!e.getTentEngine() && moved.getTentEngine());
    assert(!moved.freeEngine());
}

static void policy() {
    auto cfg = config();
    native::json rules = native::json::parse(R"([
        {"name":"background","segment_type":"memory","intent_type":"background_prefetch","transports":["tcp"]},
        {"name":"no_route","segment_type":"memory","transports":["nvlink"]}])");
    cfg->set("policy", rules);
    native::TransferEngine e(cfg);
    assert(e.available());
    Bytes bytes;
    bytes.fill('!');
    std::memcpy(bytes.data() + 2, "data", 4);
    ok(e.registerLocalMemory(bytes.data(), bytes.size()));
    auto b = e.allocateBatch(4);
    auto r = request(native::Request::READ, bytes.data() + 16, 0,
                     reinterpret_cast<uintptr_t>(bytes.data() + 2), 4);
    auto bad = r;
    bad.policy_name = "unknown";
    assert(!e.submitTransfer(b, {r, bad}).ok());
    assert(!e.submitTransfer(b, {r}).ok());
    r.intent_type = native::IntentType::BACKGROUND_PREFETCH;
    bad = r;
    bad.policy_name = "no_route";
    assert(!e.submitTransfer(b, {bad}).ok());
    std::vector<native::TransferStatus> tasks;
    ok(e.getTransferStatus(b, tasks));
    assert(tasks.empty());
    r.policy_name = "background";
    // An expired absolute deadline must reach the owner even on a healthy loopback path.
    bad = r;
    bad.deadline_ns = 1;
    auto timeout = e.submitTransfer(b, {bad});
    if (timeout.ok()) {
        native::TransferStatus t{};
        do {
            ok(e.progressBatch(b, t));
        } while (t.s == native::PENDING);
        assert(t.s == native::TIMEOUT || t.s == native::CANCELED);
        assert(!e.waitTransferCompletion(b).ok());
    } else
        ok(e.freeBatch(b));
    assert(std::all_of(bytes.begin() + 16, bytes.end(), [](auto v) { return v == '!'; }));
    r.deadline_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            (std::chrono::steady_clock::now() + std::chrono::seconds(10)).time_since_epoch())
            .count();
    ok(e.transferSync({r}));
    Bytes expected;
    expected.fill('!');
    std::memcpy(expected.data() + 2, "data", 4);
    std::memcpy(expected.data() + 16, "data", 4);
    assert(bytes == expected);
    ok(e.unregisterLocalMemory(bytes.data()));
}
static void contracts(const std::string &path) {
    tent_set_config("rpc_server_hostname", "127.0.0.9");
    native::TransferEngine from_path(path);
    assert(from_path.available() && from_path.getRpcServerAddress() == "127.0.0.3");
    native::TransferEngine missing(path + ".missing");
    assert(!missing.available());
    auto conf = config("127.0.0.2");
    native::TransferEngine first(conf);
    conf->set("rpc_server_hostname", "127.0.0.4");
    native::TransferEngine second(conf);
    assert(first.getRpcServerAddress() == "127.0.0.2" &&
           second.getRpcServerAddress() == "127.0.0.4");
    native::TransferEngine defaults;
    assert(defaults.available() && defaults.getRpcServerAddress() != "127.0.0.9");
    native::SegmentID id = 734;
    std::string name = "untouched";
    assert(first.exportLocalSegment(name).IsNotImplemented() && name == "untouched");
    assert(first.importRemoteSegment(id, name).IsNotImplemented() && id == 734);
    void *host = nullptr, *more = nullptr;
    native::MemoryOptions o;
    o.perm = native::kGlobalReadWrite;
    o.type = native::TCP;
    ok(first.allocateLocalMemory(&host, 16, "*"));
    ok(first.allocateLocalMemory(&more, 16, o));
    ok(first.registerLocalMemory({host, more}, {16, 16}, o));
    ok(first.unregisterLocalMemory({host, more}, {16, 16}));
    ok(first.registerLocalMemory(host, 16, o));
    ok(first.unregisterLocalMemory(host, 16));
    ok(first.freeLocalMemory(host));
    ok(first.freeLocalMemory(more));
    std::vector<native::NicLoadStats> nics;
    ok(first.getNicLoadStats(nics));
    native::Topology value;
    ok(value.parse(
        R"({"nics":[{"name":"railA","type":0,"numa_node":0},{"name":"railB","type":0,"numa_node":1}],"mems":[{"name":"cpu:0","type":0,"numa_node":0,"device_list":{"rank0":[0],"rank1":[],"rank2":[1]}}]})"));
    assert(value.getMemCount() == 1 && value.getNicCount() == 2 && value.getNicName(1) == "railB" &&
           value.isCrossNuma(*value.getMemEntry(0), 1));
    tent_engine_t raw = orbitte_create_v1(2, path.c_str());
    assert(tent_available(raw));
    auto b = tent_allocate_batch(raw, 2);
    Bytes v{};
    assert(!tent_register_memory(raw, v.data(), v.size()));
    alignas(orbitte_request_v1) uint32_t short_header = sizeof(uint32_t);
    assert(orbitte_submit_v1(raw, b, reinterpret_cast<const orbitte_request_v1 *>(&short_header), 1,
                             nullptr, 0) == -1);
    orbitte_request_v1 row{};
    row.struct_size = sizeof row;
    row.abi_version = 1;
    row.request = {OPCODE_READ, v.data() + 8, 0, reinterpret_cast<uintptr_t>(v.data()), 4,
                   0,           TRANSPORT_TCP};
    for (int i = 0; i < 4; ++i) {
        auto invalid = row;
        if (!i)
            invalid.struct_size--;
        else if (i == 1)
            invalid.abi_version++;
        else if (i == 2)
            invalid.reserved = 1;
        else
            invalid.intent_type = 7;
        orbitte_request_v1 entries[] = {row, invalid};
        assert(orbitte_submit_v1(raw, b, entries, 2, nullptr, 0) == -1);
        size_t count = 0;
        tent_status_t out{};
        assert(!tent_task_status_list(raw, b, &out, &count) && count == 0);
    }
    assert(!tent_free_batch(raw, b) && !tent_unregister_memory(raw, v.data(), v.size()));
    tent_destroy_engine(raw);
}

static void scatter(const std::string &name, bool hp) {
    classic::TransferEngine e;
    assert(e.init(P2PHANDSHAKE, "") == 0);
    auto peer = e.openSegment(name);
    assert(peer != UINT64_MAX);
    std::vector<classic::SegmentBufferInfo> regions;
    assert(!e.getSegmentBuffers(peer, regions));
    const auto remote =
        std::find_if(regions.begin(), regions.end(), [](const auto &b) { return b.length == 32; });
    assert(remote != regions.end());
    Bytes bytes;
    bytes.fill('!');
    std::memcpy(bytes.data() + 20, "WRITE", 5);
    assert(!e.registerLocalMemory(bytes.data(), bytes.size()));
    if (hp) {
        classic::TransferRequest ordinary{};
        ordinary.opcode = classic::TransferRequest::READ;
        ordinary.source = bytes.data() + 3;
        ordinary.target_id = peer;
        ordinary.target_offset = remote->addr + 4;
        ordinary.length = 6;
        ordinary.transport_hint = TRANSPORT_HP_TCP;
        auto b = e.allocateBatchID(1);
        assert(!e.submitTransfer(b, {ordinary}).ok());
        ok(e.freeBatchID(b));
    }
    std::array<size_t, 2> local{3, 1}, offset{4, 0}, length{6, 0};
    int delivered = 0, empty = 0;
    classic::TransferEngine::ScatterTransferRange r{classic::TransferRequest::READ,
                                                    name,
                                                    remote->addr,
                                                    32,
                                                    bytes.data(),
                                                    bytes.size(),
                                                    local,
                                                    offset,
                                                    length,
                                                    [&](size_t i, const classic::Status &s) {
                                                        if (!i) {
                                                            ++delivered;
                                                            assert(hp ? s.ok()
                                                                      : s.IsNotImplemented());
                                                        } else {
                                                            assert(i == 1 && s.ok());
                                                            ++empty;
                                                        }
                                                    },
                                                    2};
    auto op = e.submitScatter({r});
    if (hp) {
        std::cout << "submitted" << std::endl;
        std::string line;
        std::getline(std::cin, line);
        assert(line == "pending");
        assert(op.waitFor(std::chrono::nanoseconds::zero()).IsClock());
        assert(!delivered && empty == 1 && bytes[3] == '!');
        auto moved = std::move(op);
        assert(op.wait().IsInvalidArgument());
        std::cout << "held" << std::endl;
        ok(moved.wait());
        ok(moved.wait());
        assert(delivered == 1 && empty == 1);
        Bytes want;
        want.fill('!');
        std::memcpy(want.data() + 3, "source", 6);
        std::memcpy(want.data() + 20, "WRITE", 5);
        assert(bytes == want);
        std::array<size_t, 1> wl{20}, wr{13}, wn{5};
        classic::TransferEngine::ScatterTransferRange write{classic::TransferRequest::WRITE,
                                                            name,
                                                            remote->addr,
                                                            32,
                                                            bytes.data(),
                                                            bytes.size(),
                                                            wl,
                                                            wr,
                                                            wn,
                                                            {},
                                                            0};
        // The operation keeps its engine across destruction of the facade.
        auto live = e.getTentEngine();
        auto w = e.submitScatter({write});
        assert(!e.freeEngine());
        ok(w.wait());
        ok(live->probePeerAliveByID(peer));
        ok(live->unregisterLocalMemory(bytes.data()));
        ok(live->closeSegment(peer));
    } else {
        assert(op.wait().IsNotImplemented());
        assert(empty == 1 && delivered == 1 && bytes[3] == '!');
        ok(e.CheckSegmentStatus(peer));
        assert(!e.closeSegment(peer));
        assert(!e.unregisterLocalMemory(bytes.data()));
    }
}
int main(int argc, char **argv) {
    assert(argc >= 2);
    std::string cmd = argv[1];
    if (cmd == "scatter" || cmd == "no-scatter")
        scatter(argv[2], cmd == "scatter");
    else if (cmd == "signal") {
        classic::TransferEngine e;
        e.enableGracefulShutdown();
        assert(!e.init(P2PHANDSHAKE, ""));
        std::cout << "ready" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(30));
        return 3;
    } else if (cmd == "source")
        source(argc == 3 && std::string(argv[2]) == "classic");
    else if (cmd == "native")
        native_client(argv[2]);
    else if (cmd == "classic")
        classic_client(argv[2]);
    else if (cmd == "policy")
        policy();
    else if (cmd == "contracts")
        contracts(argv[2]);
    else
        return 2;
}
