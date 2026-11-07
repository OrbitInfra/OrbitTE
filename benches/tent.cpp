// Test-only benchmark against the frozen TENT runtime; never linked into OrbiTE.
#include "tent/transfer_engine.h"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include <cstdlib>
#include <ctime>

namespace {
using Clock = std::chrono::steady_clock;
constexpr size_t page = 4096, limit = 64 << 20;
// Fail the process without unwinding registered backing after an uncertain native result.
void check(bool ok, const char* message) { if (!ok) { std::cerr << message << '\n'; std::_Exit(1); } }
void checked(int status) { check(status == 0, "TENT operation failed"); }
uint64_t cpu_ns() {
    timespec t{}; check(clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t) == 0, "CPU clock");
    return uint64_t(t.tv_sec)*1000000000 + t.tv_nsec;
}
size_t number(const char* s) { char* end{}; auto n = std::strtoull(s, &end, 10); check(*s && !*end, "invalid number"); return n; }
void file_write(const std::string& path, const std::vector<unsigned char>& bytes) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    f.write(reinterpret_cast<const char*>(bytes.data()), bytes.size()); f.close(); check(!f.fail(), "file setup failed");
}
std::vector<unsigned char> file_read(const std::string& path) {
    std::ifstream f(path, std::ios::binary); check(f.good(), "file verification failed");
    return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}
void array(const std::vector<uint64_t>& samples) { std::cout << '['; for (size_t i=0;i<samples.size();++i) std::cout << (i ? "," : "") << samples[i]; std::cout << ']'; }
}
int main(int argc, char** argv) {
    check(argc == 10, "backend read|write block batch contiguous|gapped warmup samples workers file");
    const std::string backend = argv[1], op = argv[2], layout = argv[5], path = argv[9];
    const auto block=number(argv[3]), batch=number(argv[4]), warmup=number(argv[6]), samples=number(argv[7]), workers=number(argv[8]);
    check((backend=="cpu" || backend=="tcp" || backend=="io_uring") && (op=="read" || op=="write") && (layout=="contiguous" || layout=="gapped"), "invalid profile");
    check(block>=page && block<=limit && block%page==0 && batch>=1 && batch<=64 && block*batch<=limit && warmup>=1 && warmup<=100 && samples>=1 && samples<=10000 && workers>=1 && workers<=16, "invalid bounds");
    const auto stride=block*(layout=="gapped" ? 2 : 1), extent=(batch-1)*stride+block, capacity=extent+3*page;
    auto* local=static_cast<unsigned char*>(std::aligned_alloc(page, capacity));
    auto* remote=static_cast<unsigned char*>(std::aligned_alloc(page, capacity));
    check(local && remote, "aligned allocation");
    const bool file=backend=="io_uring", cpu=backend=="cpu";
    tent_engine_t source=nullptr, requester=nullptr;
    tent_segment_id_t peer{};
    if (!cpu) {
        tent_set_config("metadata_type", "p2p");
        tent_set_config("rpc_server_hostname", "127.0.0.1");
        tent_set_config("rpc_server_port", "0");
        tent_set_config("rpc_server_threads", argv[8]);
        tent_set_config("transports/tcp/max_concurrent_tasks", argv[8]);
        // Pin the frozen transport-local defaults separately from engine failover.
        tent_set_config("transports/tcp/max_retry_count", "3");
        tent_set_config("transports/tcp/retry_base_delay_ms", "100");
        tent_set_config("transports/tcp/retry_max_delay_ms", "2000");
        tent_set_config("log_level", "error");
        tent_set_config("enable_progress_worker", "false");
        tent_set_config("enable_runtime_queue", "false");
        tent_set_config("merge_requests", "true");
        tent_set_config("max_failover_attempts", "0");
        tent_set_config("transports/io_uring/pinned_staging", "false");
        for (const auto* name : {"tcp", "rdma", "shm", "hp_tcp", "io_uring", "nvlink", "mnnvl", "gds", "ascend_direct", "sunrise_link", "mpcomm", "tpu", "ub", "xpu", "hylink", "fabric", "mlu", "memfabric"}) {
            const auto key=std::string("transports/")+name+"/enable";
            tent_set_config(key.c_str(), (backend==name || (file && std::string(name)=="tcp")) ? "true" : "false");
        }
        source=tent_create_engine(); requester=tent_create_engine();
        check(source && requester && tent_available(source)>0 && tent_available(requester)>0, "TENT engines unavailable");
        checked(tent_register_memory(source, remote, capacity));
        checked(tent_register_memory(requester, local, capacity));
        if (file) { file_write(path, std::vector<unsigned char>(extent+2*page, 0xa5)); checked(tent_open_segment(requester, &peer, ("file://"+path).c_str())); }
        else { char name[256]{}; checked(tent_segment_name(source, name, sizeof(name))); checked(tent_open_segment(requester, &peer, name)); }
    }
    std::vector<tent_request_t> requests(batch);
    for (size_t b=0;b<batch;++b) {
        requests[b].opcode=op=="read" ? OPCODE_READ : OPCODE_WRITE;
        requests[b].source=local+page+b*stride;
        requests[b].target_id=peer;
        requests[b].target_offset=(file ? page : reinterpret_cast<uint64_t>(remote+page))+b*stride;
        requests[b].length=block;
        requests[b].transport_hint=file ? TRANSPORT_IOURING : TRANSPORT_TCP;
    }
    std::vector<uint64_t> times, cpus;
    for (size_t i=0;i<warmup+samples;++i) {
        std::vector<unsigned char> image(extent, 0xa5);
        for(size_t b=0;b<batch;++b) for(size_t j=0;j<block;++j) image[b*stride+j]=(j*17+b*37+i*13)%251;
        std::memset(local, 0xa5, capacity); std::memset(remote, 0xa5, capacity);
        std::memcpy((op=="write" || cpu ? local : remote)+page, image.data(), extent);
        if(file) { std::vector<unsigned char> bytes(extent+2*page, 0xa5); if(op=="read") std::copy(image.begin(), image.end(), bytes.begin()+page); file_write(path, bytes); }
        const auto c=cpu_ns(); const auto start=Clock::now();
        if(cpu) { for(size_t b=0;b<batch;++b) std::memcpy(remote+page+b*stride, local+page+b*stride, block); }
        else {
            const auto id=tent_allocate_batch(requester, batch); check(id!=0, "batch allocation");
            checked(tent_submit(requester, id, requests.data(), batch));
            size_t polls=0;
            for (;;) {
                tent_status_t s{}; checked(tent_overall_status(requester, id, &s));
                if(s.status==STATUS_COMPLETED) { check(s.transferred_bytes==block*batch, "short batch"); break; }
                check(s.status==STATUS_PENDING || s.status==STATUS_WAITING, "failed benchmark task");
                check(Clock::now()-start<std::chrono::seconds(30), "benchmark deadline");
                if(polls>=128) std::this_thread::sleep_for(std::chrono::microseconds(std::min<uint64_t>(200, uint64_t{1}<<std::min<size_t>(8, polls-128))));
                ++polls;
            }
            checked(tent_free_batch(requester, id));
        }
        const auto elapsed=std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-start).count();
        const auto cost=cpu_ns()-c;
        auto verify=[&](const unsigned char* bytes, bool populated) {
            for(size_t j=0;j<capacity;++j) check(bytes[j]==(populated && j>=page && j<page+extent ? image[j-page] : 0xa5), "bytes or guards differ");
        };
        if(std::getenv("ORBITE_BENCH_CORRUPT_BYTE")) local[0]=0;
        verify(local, true); verify(remote, !file || op=="read");
        if(file) { std::vector<unsigned char> expected(extent+2*page, 0xa5); std::copy(image.begin(), image.end(), expected.begin()+page); check(file_read(path)==expected, "file bytes or guards differ"); }
        if(i>=warmup) { times.push_back(elapsed); cpus.push_back(cost); }
    }
    const auto threads=std::distance(std::filesystem::directory_iterator("/proc/self/task"), std::filesystem::directory_iterator{});
    if(!cpu) {
        checked(tent_close_segment(requester, peer));
        checked(tent_unregister_memory(requester, local, capacity)); checked(tent_unregister_memory(source, remote, capacity));
        tent_destroy_engine(requester); tent_destroy_engine(source);
    }
    std::free(local); std::free(remote);
    std::cout << "{\"verified\":true,\"payload_bytes\":" << block*batch << ",\"process_threads\":" << threads << ",\"physical_owners\":null,\"direct_io\":null,\"samples_ns\":";
    array(times); std::cout << ",\"cpu_ns\":"; array(cpus); std::cout << "}\n";
}
