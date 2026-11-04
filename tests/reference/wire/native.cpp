// Test-only peer and serializer. All native types and codecs come from the
// frozen tree. No provider is installed and no fixture address is dereferenced.
#include "tent/common/config_lifecycle.h"
#include "tent/common/utils/string_builder.h"
#include "tent/metrics/config_loader.h"
#include "tent/runtime/control_plane.h"
#include "tent/transfer_engine.h"
#include "tent/transport/fabric/fabric_attrs.h"
#include "tent/transport/hp_tcp/hp_tcp_protocol.h"
#include "tent/transport/mpcomm/mpcomm_peer_registry.h"

#include <iostream>
#include <stdexcept>
#include <string>

using namespace mooncake::tent;

namespace {
void require(bool ok, const char* why) {
    if (!ok) throw std::runtime_error(why);
}
void check(Status status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}

constexpr uint64_t address = 0x0102030405060708ULL;
const std::string payload("a\0z", 3);
BootstrapDesc bootstrap() {
    return {"left/rail0", "right/rail1", {17, 18}, 7, "fe80::1", "", 19};
}

SegmentDesc memory() {
    SegmentDesc seg{};
    seg.name = "source";
    seg.type = SegmentType::Memory;
    seg.machine_id = "host-A";
    seg.rpc_server_addr = "127.0.0.1:19000";
    seg.rdma_server_name = "rail-A";
    auto& m = std::get<MemorySegmentDesc>(seg.detail);
    check(m.topology.parsePriorityMatrix(R"({"cpu:0":[["rail0"],[]]})"));
    m.device_attrs["rack"] = "A";
    m.devices.push_back({"rail0", {{RDMA, "device-attr"}}, 7, "fe80::1"});

    HighPerformanceTcpEndpointAttr ep{
        "00112233445566778899aabbccddeeff",
        {{"127.0.0.1", 19001}, {"127.0.0.2", 19002}},
        4096};
    check(EncodeHighPerformanceTcpEndpointAttr(ep, &m.transport_attrs[HP_TCP]));
    BufferDesc b{};
    b.addr = address;
    b.length = 64;
    b.location = "cpu:0";
    b.regions = {{64, "cpu:0"}};
    b.transports = {HP_TCP, RDMA};
    b.rkey = {31, 32};
    b.lkey = {99};  // local registration state must never enter JSON
    b.ref_count = 3;
    b.permission = kLocalReadWrite;
    check(EncodeHighPerformanceTcpBufferAttr(
        {9, HighPerformanceTcpPermissionName(kGlobalReadWrite)},
        &b.transport_attrs[HP_TCP]));
    m.buffers.push_back(b);
    return seg;
}

json formats() {
    SegmentDesc file{};
    file.name = "disk";
    file.type = SegmentType::File;
    file.machine_id = "host-A";
    file.rpc_server_addr = "127.0.0.1:19000";
    file.detail = FileSegmentDesc{{{"/sample", 256, 128}}};
    json mem = memory(), disk = file;
    require(json(mem.get<SegmentDesc>()) == mem &&
                json(disk.get<SegmentDesc>()) == disk,
            "native segment round trip");
    auto old = mem;
    old.erase("rdma_server_name");
    old["detail"].erase("topology");
    old["detail"].erase("transport_attrs");
    require(old.get<SegmentDesc>().nicPathServerName() == "source",
            "optional metadata");
    auto bad = mem;
    bad.erase("machine_id");
    bool rejected = false;
    try {
        (void)bad.get<SegmentDesc>();
    } catch (const json::exception&) {
        rejected = true;
    }
    require(rejected, "missing required metadata accepted");

    HighPerformanceTcpRequestFrame req{HighPerformanceTcpOpcode::kRead, address,
                                       9, 256, 3};
    auto read = EncodeHighPerformanceTcpRequest(req);
    req.opcode = HighPerformanceTcpOpcode::kWrite;
    auto write = EncodeHighPerformanceTcpRequest(req);
    auto ack = EncodeHighPerformanceTcpResponse(
        {HighPerformanceTcpStatus::kOk, address, 3});
    auto hex = [](const auto& a) {
        return hexEncode(std::string(a.begin(), a.end()));
    };
    auto corrupt = read;
    corrupt.front() ^= 1;
    require(
        !DecodeHighPerformanceTcpRequest(corrupt.data(), corrupt.size(), &req)
             .ok(),
        "bad HP-TCP magic accepted");

    FabricPeerAttr peer{};
    peer.provider = "tcp";
    peer.virt_addr = false;
    peer.nics = {{"nic-A", std::string("\0\xff", 2), 0}};
    FabricBufferAttr buf{{{0, 64, {0}, {0x100000002ULL}}}};
    auto fabric = encodeFabricPeerAttr(peer),
         chunks = encodeFabricBufferAttr(buf);
    check(decodeFabricPeerAttr(fabric, peer));
    check(decodeFabricBufferAttr(chunks, 64, buf));
    require(!decodeFabricBufferAttr(chunks, 65, buf).ok(),
            "uncovered fabric byte accepted");
    std::string host;
    int port = 0;
    auto mp = buildMpcommEndpointAttr("192.0.2.1:1234", 4321);
    check(parseMpcommEndpointAttr(mp, host, port));
    require(host == "192.0.2.1" && port == 4321,
            "MPComm endpoint did not round trip");
    check(parseMpcommEndpointAttr("192.0.2.1:4321", host, port));
    require(host == "192.0.2.1" && port == 4321, "legacy MPComm endpoint");
    require(!parseMpcommEndpointAttr("v2:192.0.2.1:4321", host, port).ok(),
            "MPComm prefix");
    auto bad_boot = bootstrap();
    bad_boot.local_gid.clear();
    require(
        !ControlClient::decodeBootstrapResponse(json(bad_boot).dump(), bad_boot)
             .ok(),
        "missing peer GID accepted");
    std::array<uint8_t, 64> handle{};
    for (size_t i = 0; i < handle.size(); ++i)
        handle[i] = static_cast<uint8_t>(255 - i);
    auto ipc = serializeBinaryData(handle.data(), handle.size());
    std::vector<unsigned char> decoded;
    deserializeBinaryData(ipc, decoded);
    require(decoded.size() == handle.size() &&
                std::equal(decoded.begin(), decoded.end(), handle.begin()),
            "opaque GPU handle encoding");
    return {{"memory", mem},
            {"file", disk},
            {"rdma", bootstrap()},
            {"opaque_cuda_handle", ipc},
            {"fabric_peer", json::parse(fabric)},
            {"fabric_buffer", json::parse(chunks)},
            {"mpcomm", mp},
            {"hp_tcp",
             {{"read", hex(read)}, {"write", hex(write)}, {"ack", hex(ack)}}}};
}

// Run each precedence vector in a fresh child, with its own environment.
void observe_config() {
    json input;
    std::cin >> input;
    auto config = std::make_shared<Config>();
    check(config->load(input.value("base", json::object()).dump()));
    bool ok = true;
    json values = json::object();
    if (input["mode"] == "metrics") {
        auto m = MetricsConfigLoader::loadWithDefaults(config.get());
        values = {{"enabled", m.enabled},
                  {"http_port", m.http_port},
                  {"http_host", m.http_host},
                  {"http_server_threads", m.http_server_threads},
                  {"report_interval_seconds", m.report_interval_seconds}};
    } else {
        if (input["mode"] == "engine") {
            TransferEngine engine(config);
            ok = engine.available();
        } else {
            ok = ConfigHelper().loadFromEnv(*config).ok();
            if (input["mode"] == "lifecycle" && ok)
                ok = buildTentConfigBundle(*config, 1).diagnostics.empty();
        }
        for (auto it = input["expect"].begin(); it != input["expect"].end();
             ++it)
            values[it.key()] = config->get<json>(it.key(), nullptr);
    }
    std::cout << json{{"ok", ok}, {"values", values}}.dump() << '\n';
}

// The application-level TCP controls also replay into actual engine handlers.
void serve_registered() {
    auto cfg = std::make_shared<Config>();
    cfg->set("rpc_server_hostname", std::string("127.0.0.1"));
    cfg->set("local_segment_name", std::string("freeze-test"));
    ConfigHelper::forceTcp(*cfg);
    cfg->set("transports/io_uring/enable", false);
    cfg->set("metrics/enabled", false);
    std::array<char, 5> bytes{'<', 'o', 'l', 'd', '>'};
    TransferEngine engine(cfg);
    require(engine.available(), "registered source did not start");
    check(engine.registerLocalMemory(bytes.data() + 1, 3, kGlobalReadWrite));
    std::cout << engine.getRpcServerPort() << ' '
              << reinterpret_cast<uintptr_t>(bytes.data() + 1) << std::endl;
    std::cin.get();
    require(bytes.front() == '<' && bytes.back() == '>' &&
                std::string(bytes.data() + 1, 3) == payload,
            "WRITE bytes or guards differ");
    check(engine.unregisterLocalMemory(bytes.data() + 1, 3));
}

void serve() {
    CoroRpcAgent rpc;
    // Deterministic attachments stand in for storage, staging and RNIC owners.
    // The surrounding server, dispatcher, client codecs and transport are real.
    for (int id = GetSegmentDesc; id <= BootstrapUb; ++id) {
        check(rpc.registerFunction(id, [id](const std::string_view& input,
                                            std::string& out) {
            switch (id) {
                case GetSegmentDesc:
                    out = json(memory()).dump();
                    break;
                case BootstrapRdma:
                    out = json(json::parse(input).get<BootstrapDesc>()).dump();
                    break;
                case BootstrapUb:
                    out =
                        json(json::parse(input).get<UbBootstrapDesc>()).dump();
                    break;
                case RecvData:
                    out = payload;
                    break;
                case Pin:
                    out = json(8192).dump();
                    break;
                default:
                    break;
            }
        }));
    }
    uint16_t port = 0;
    check(rpc.start(port));
    std::cout << port << std::endl;
    std::cin.get();
    check(rpc.stop());
}

void call(int id, const std::string& peer) {
    std::string response, bytes = payload;
    switch (id) {
        case GetSegmentDesc:
            check(ControlClient::getSegmentDesc(peer, response));
            require(json::parse(response) == json(memory()), "segment reply");
            break;
        case BootstrapRdma: {
            BootstrapDesc b;
            check(ControlClient::bootstrap(peer, bootstrap(), b));
            require(json(b) == json(bootstrap()), "RDMA reply");
            break;
        }
        case SendData:
            check(ControlClient::sendData(peer, address, bytes.data(),
                                          bytes.size()));
            break;
        case RecvData:
            bytes.assign(3, '?');
            check(ControlClient::recvData(peer, address, bytes.data(),
                                          bytes.size()));
            require(bytes == payload, "TCP READ reply");
            break;
        case Notify:
            check(ControlClient::notify(peer, {"session", "a\"b"}));
            break;
        case Probe:
            check(ControlClient::probe(peer));
            break;
        case Delegate: {
            Request r{};
            r.opcode = Request::WRITE;
            r.source = reinterpret_cast<void*>(0x1000);
            r.target_id = 7;
            r.target_offset = 8;
            r.length = 3;
            r.policy_name = "must-not-serialize";
            r.priority = PRIO_LOW;
            check(ControlClient::delegate(peer, r));
            break;
        }
        case Pin: {
            uint64_t addr = 0;
            check(ControlClient::pinStageBuffer(peer, "cpu:0", addr));
            require(addr == 8192, "pin reply");
            break;
        }
        case Unpin:
            check(ControlClient::unpinStageBuffer(peer, 8192));
            break;
        case SubscribeSegmentUpdate:
            ControlClient::subscribeSegmentUpdateAsync(peer, "peer:1234");
            break;
        case NotifySegmentUpdated:
            ControlClient::notifySegmentUpdatedAsync(peer, "source", [] {});
            break;
        case BootstrapUb: {
            UbBootstrapDesc req{}, resp{};
            req.segment_name = "excluded";
            req.jetty_ids = {7};
            req.jetty_uasids = {9};
            check(ControlClient::bootstrapUb(peer, req, resp));
            require(resp.jetty_ids == req.jetty_ids, "UB envelope");
            break;
        }
        default: {
            CoroRpcAgent agent;
            require(!agent.call(peer, id, "", response).ok(),
                    "unknown logical function accepted");
        }
    }
    std::cin
        .get();  // Keep async clients alive until the relay forwards the reply.
}
}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 2 && std::string(argv[1]) == "formats")
            std::cout << formats().dump() << '\n';
        else if (argc == 2 && std::string(argv[1]) == "config")
            observe_config();
        else if (argc == 2 && std::string(argv[1]) == "server")
            serve();
        else if (argc == 2 && std::string(argv[1]) == "registered")
            serve_registered();
        else if (argc == 4 && std::string(argv[1]) == "client")
            call(std::stoi(argv[2]), argv[3]);
        else
            return 2;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
