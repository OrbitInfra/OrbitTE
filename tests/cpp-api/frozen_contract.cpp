// Compile the same source against v0.3.14 and the installed OrbiTE headers.
#include <transfer_engine.h>
#include <tent/transfer_engine.h>
#include <tent/common/config.h>
#include <tent/runtime/topology.h>
#include <tent/runtime/transfer_engine_impl.h>
#include <concepts>
#include <type_traits>
using Native = mooncake::tent::TransferEngine;
using Classic = mooncake::TransferEngine;
namespace t = mooncake::tent;
static_assert(std::is_constructible_v<Native> && std::is_constructible_v<Native, std::string> &&
              std::is_constructible_v<Native, std::shared_ptr<t::Config>> &&
              !std::is_copy_constructible_v<Native>);
static_assert(requires(Native &e, t::BatchID b, t::SegmentID p, std::vector<t::Request> &r,
                       t::TransferStatus &s, std::vector<t::TransferStatus> &list,
                       t::Notification &n, t::SegmentInfo &info) {
    { e.submitTransfer(b, r) } -> std::same_as<t::Status>;
    e.submitTransfer(b, r, n);
    e.getTransferStatus(b, s);
    e.getTransferStatus(b, 0, list.emplace_back());
    e.getTransferStatus(b, list);
    e.progressBatch(b, s);
    e.probePeerAliveByID(p);
    e.getSegmentInfo(p, info);
    e.getLocalTopology()->getNicCount();
    e.getLocalTopologyString();
    r[0].policy_name = "cache";
    r[0].intent_type = t::IntentType::CHECKPOINT;
    r[0].deadline_ns = 123;
});
static_assert(std::is_nothrow_move_constructible_v<Classic>);
static_assert(requires(Classic &e, mooncake::BatchID b, mooncake::SegmentID p,
                       std::vector<mooncake::TransferRequest> &r, mooncake::TransferStatus &s,
                       std::vector<Classic::ScatterTransferRange> &fragments,
                       std::vector<mooncake::SegmentBufferInfo> &buffers) {
    e.init("P2PHANDSHAKE", "");
    e.getTentEngine();
    e.getSegmentBuffers(p, buffers);
    e.submitTransfer(b, r);
    e.getTransferStatus(b, 0, s);
    e.getBatchTransferStatus(b, s);
    e.submitScatter(fragments).waitFor(std::chrono::nanoseconds(0));
    e.transferScatter(fragments);
    e.sendNotifyByID(p, {});
    e.getLocalTopologyString();
    e.freeBatchID(b);
});

static_assert(requires(t::TransferEngineImpl &e, t::BatchID b, std::vector<t::Request> &requests) {
    { e.waitTransferCompletion(b) } -> std::same_as<t::Status>;
    { e.transferSync(requests) } -> std::same_as<t::Status>;
    { t::nextPollDelay(UINT64_MAX) } -> std::same_as<std::chrono::microseconds>;
});
