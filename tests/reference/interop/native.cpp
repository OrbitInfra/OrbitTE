// Mixed-peer gate only. Compiled against the isolated frozen TENT, never OrbiTE's libtent.
#include "tent/transfer_engine.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

static void need(bool ok, const char* what) { if (!ok) throw std::runtime_error(what); }
static void check(int code) { need(code == 0, "native API rejected operation"); }
static unsigned char byte(size_t i, bool written) { return static_cast<unsigned char>((i * 17 + (written ? 91 : 23)) % 251); }
static void config() {
    tent_set_config("metadata_type", "p2p");
    tent_set_config("rpc_server_hostname", "127.0.0.1");
    tent_set_config("rpc_server_port", "0");
    tent_set_config("rpc_server_threads", "4");
    tent_set_config("log_level", "error");
    tent_set_config("enable_progress_worker", "false");
    tent_set_config("enable_runtime_queue", "false");
    for (const auto* t : {"tcp", "rdma", "shm", "hp_tcp", "io_uring", "nvlink", "mnnvl", "gds", "ascend_direct", "sunrise_link", "mpcomm", "tpu", "ub", "xpu", "hylink", "fabric", "mlu", "memfabric"}) {
        const auto key = std::string("transports/") + t + "/enable";
        tent_set_config(key.c_str(), std::string(t) == "tcp" ? "true" : "false");
    }
}
static bool transfer(tent_engine_t engine, tent_segment_id_t peer, void* local, uint64_t remote, size_t size, bool write) {
    const auto batch=tent_allocate_batch(engine,1); need(batch!=0,"batch allocation");
    tent_request_t req{}; req.opcode=write?OPCODE_WRITE:OPCODE_READ; req.source=local; req.target_id=peer; req.target_offset=remote; req.length=size; req.transport_hint=TRANSPORT_TCP;
    const int submitted=tent_submit(engine,batch,&req,1);
    bool complete=false;
    if(submitted==0) {
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
        for(;;) {
            tent_status_t s{}; check(tent_task_status(engine,batch,0,&s));
            if(s.status==STATUS_COMPLETED) { need(s.transferred_bytes==size,"native byte count");complete=true;break; }
            if(s.status!=STATUS_PENDING && s.status!=STATUS_WAITING) break;
            need(std::chrono::steady_clock::now()<end,"native timeout"); std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
    check(tent_free_batch(engine,batch)); return complete;
}
int main(int argc,char** argv) {
    try {
        need(argc>=3,"source size | requester endpoint base readonly size"); config();
        const bool source=std::string(argv[1])=="source";
        const size_t n=std::stoull(argv[source?2:5]); need(n>0 && n<=(1<<20),"size bound");
        // Backing outlives engine on every path, including exceptions.
        std::vector<unsigned char> bytes(n+2,0xa1), ro(n+2,0xb2);
        if(source) for(size_t i=0;i<n;++i) bytes[i+1]=byte(i,false);
        std::unique_ptr<void,decltype(&tent_destroy_engine)> engine(tent_create_engine(),tent_destroy_engine);
        need(engine && tent_available(engine.get()),"native engine unavailable");
        check(tent_register_memory_with_perm(engine.get(),bytes.data(),bytes.size(),source?PERM_GLOBAL_READ_WRITE:PERM_LOCAL_READ_WRITE));
        if(source) {
            check(tent_register_memory_with_perm(engine.get(),ro.data(),ro.size(),PERM_GLOBAL_READ_ONLY));
            char host[256]{};uint16_t port=0;check(tent_rpc_server_addr_port(engine.get(),host,sizeof(host),&port));
            std::cout<<"READY "<<host<<":"<<port<<" "<<reinterpret_cast<uint64_t>(bytes.data())<<" "<<reinterpret_cast<uint64_t>(ro.data())<<" "<<n<<std::endl;
            std::string command; std::getline(std::cin,command); need(command=="verify","missing source verification");
            need(bytes.front()==0xa1 && bytes.back()==0xa1,"source guards");
            for(size_t i=0;i<n;++i) need(bytes[i+1]==byte(i,true),"source WRITE bytes");
            need(std::all_of(ro.begin(),ro.end(),[](auto b){return b==0xb2;}),"read-only source changed");
            check(tent_unregister_memory(engine.get(),ro.data(),ro.size()));
        } else {
            tent_segment_id_t peer=0;check(tent_open_segment(engine.get(),&peer,argv[2]));
            tent_segment_info_t info{};check(tent_get_segment_info(engine.get(),peer,&info));need(info.type==TYPE_MEMORY && info.num_buffers==2,"native descriptor");tent_free_segment_info(&info);
            const auto remote=std::stoull(argv[3]), readonly=std::stoull(argv[4]);
            need(transfer(engine.get(),peer,bytes.data()+1,remote+1,n,false),"native READ failed");
            for(size_t i=0;i<n;++i) need(bytes[i+1]==byte(i,false),"requester READ bytes");
            need(bytes.front()==0xa1 && bytes.back()==0xa1,"requester guards");
            for(size_t i=0;i<n;++i) bytes[i+1]=byte(i,true);
            need(transfer(engine.get(),peer,bytes.data()+1,remote+1,n,true),"native WRITE failed");
            need(!transfer(engine.get(),peer,bytes.data()+1,readonly+1,n,true),"read-only WRITE accepted");
            need(bytes.front()==0xa1 && bytes.back()==0xa1,"requester WRITE guards");
            check(tent_close_segment(engine.get(),peer));
        }
        check(tent_unregister_memory(engine.get(),bytes.data(),bytes.size()));
        std::cout<<"PASS "<<(source?"source":"requester")<<std::endl; return 0;
    }catch(const std::exception& e){std::cerr<<e.what()<<std::endl;return 1;}
}
