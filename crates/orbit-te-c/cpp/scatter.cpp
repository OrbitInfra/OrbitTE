// The operation owns its C++ callbacks and its private segment leases until drain.
#include "transfer_engine.h"
#include "tent/cpp_v1.h"
#include "tent/runtime/transfer_engine_impl.h"
#include <algorithm>
#include <limits>
#include <thread>
#include <unordered_map>

namespace mooncake {
class TransferEngine::ScatterTransferOperation::Impl {
    using Callback = std::function<void(size_t, const Status &)>;
    struct Fragment {
        size_t range, index;
    };
    std::shared_ptr<tent::TransferEngine> engine_;
    tent_engine_t handle_;
    std::unordered_map<std::string, SegmentID> peers_;
    std::vector<Callback> callbacks_;
    std::vector<Fragment> tasks_;
    std::vector<orbitte_task_v1> snapshot_;
    std::vector<bool> delivered_;
    bool abort_requested_ = false;
    BatchID batch_ = 0;
    Status outcome_;
    bool uncertain_ = false;
    bool submitted_ = false;
    void remember(const Status &s) {
        if (outcome_.ok() && !s.ok())
            outcome_ = s;
    }
    void complete(size_t r, size_t i, const Status &s) {
        remember(s);
        if (callbacks_[r])
            try {
                callbacks_[r](i, s);
            } catch (...) {
                remember(Status::Context("scatter callback threw"));
            }
    }
    void close() {
        for (auto &peer : peers_)
            if (!engine_->closeSegment(peer.second).ok())
                remember(Status::Context("close scatter segment"));
        decltype(peers_){}.swap(peers_);
        decltype(callbacks_){}.swap(callbacks_);
        decltype(tasks_){}.swap(tasks_);
        decltype(snapshot_){}.swap(snapshot_);
        decltype(delivered_){}.swap(delivered_);
        engine_.reset();
        handle_ = nullptr;
    }
    void reject(const Status &s) {
        remember(s);
        for (auto f : tasks_)
            complete(f.range, f.index, s);
        tasks_.clear();
        close();
    }

  public:
    Impl(std::shared_ptr<tent::TransferEngine> engine, tent_engine_t handle)
        : engine_(std::move(engine)), handle_(handle) {}
    void start(const std::vector<ScatterTransferRange> &ranges) {
        size_t limit = 0;
        if (!engine_ || orbitte_limits_v1(handle_, &limit)) {
            outcome_ = Status::Context("engine is unavailable");
            return;
        }
        size_t count = 0;
        if (ranges.size() > limit) {
            outcome_ = Status::TooManyRequests("scatter range count");
            return;
        }
        for (const auto &r : ranges) {
            if (r.local_offsets.size() > limit - count) {
                outcome_ = Status::TooManyRequests("scatter fragment count");
                return;
            }
            count += r.local_offsets.size();
        }
        if (!count) {
            for (const auto &r : ranges)
                if (!r.remote_offsets.empty() || !r.lengths.empty() || !r.local_buffer ||
                    r.remote_segment.empty())
                    remember(Status::InvalidArgument("invalid empty scatter range"));
            close();
            return;
        }
        // Charge the callback and descriptor lifetime before retaining either.
        {
            batch_ = engine_->allocateBatch(count);
            if (!batch_) {
                auto full = Status::TooManyRequests("allocate scatter batch");
                remember(full);
                for (const auto &r : ranges)
                    if (r.on_fragment_complete) {
                        for (size_t i = 0; i < r.local_offsets.size(); ++i)
                            try {
                                r.on_fragment_complete(i, full);
                            } catch (...) {
                            }
                    }
                return;
            }
        }
        std::vector<TransferRequest> requests;
        requests.reserve(count);
        tasks_.reserve(count);
        callbacks_.reserve(ranges.size());
        for (const auto &r : ranges)
            callbacks_.push_back(r.on_fragment_complete);
        for (size_t ri = 0; ri < ranges.size(); ++ri) {
            const auto &r = ranges[ri];
            if (r.remote_segment.empty() || r.remote_segment.size() > 4096 ||
                r.remote_segment.find('\0') != std::string::npos || !r.local_buffer ||
                r.local_offsets.size() != r.remote_offsets.size() ||
                r.local_offsets.size() != r.lengths.size()) {
                auto bad = Status::InvalidArgument("invalid scatter range");
                remember(bad);
                for (size_t i = 0; i < r.local_offsets.size(); ++i)
                    complete(ri, i, bad);
                continue;
            }
            for (size_t i = 0; i < r.lengths.size(); ++i) {
                size_t l = r.local_offsets[i], remote = r.remote_offsets[i], n = r.lengths[i];
                uintptr_t base = reinterpret_cast<uintptr_t>(r.local_buffer);
                if (l > r.local_capacity || n > r.local_capacity - l || remote > r.remote_size ||
                    n > r.remote_size - remote || l > UINTPTR_MAX - base ||
                    n > UINTPTR_MAX - (base + l) || remote > UINT64_MAX - r.remote_base_offset ||
                    n > UINT64_MAX - (r.remote_base_offset + remote) ||
                    (r.opcode != TransferRequest::READ && r.opcode != TransferRequest::WRITE) ||
                    r.intent_type < 0 || r.intent_type > 6) {
                    complete(ri, i, Status::InvalidArgument("invalid scatter fragment"));
                    continue;
                }
                if (!n) {
                    complete(ri, i, Status::OK());
                    continue;
                }
                auto [p, fresh] = peers_.try_emplace(r.remote_segment, 0);
                if (fresh) {
                    auto rc = orbitte_scatter_peer_v1(handle_, p->first.c_str(), &p->second);
                    if (rc) {
                        peers_.erase(p);
                        complete(ri, i,
                                 rc == -200 ? Status::NotImplemented("scatter file target")
                                            : Status::Metadata("open scatter segment"));
                        continue;
                    }
                }
                TransferRequest request{};
                request.opcode = r.opcode;
                request.source = reinterpret_cast<void *>(base + l);
                request.target_id = p->second;
                request.target_offset = r.remote_base_offset + remote;
                request.length = n;
                request.priority = TransferRequest::PRIO_HIGH;
                request.intent_type = r.intent_type;
                requests.push_back(request);
                tasks_.push_back({ri, i});
            }
        }
        if (requests.empty()) {
            if (batch_)
                (void)orbitte_abandon_v1(handle_, batch_);
            batch_ = 0;
            close();
            return;
        }
        snapshot_.resize(requests.size());
        delivered_.resize(requests.size());
        Status s;
        try {
            s = TransferEngine::submit(engine_, batch_, requests, nullptr, true);
        } catch (...) {
            // The C++ marshaller allocates before entering the Rust submit call.
            (void)orbitte_abandon_v1(handle_, batch_);
            batch_ = 0;
            throw;
        }
        if (!s.ok()) {
            auto retired = engine_->freeBatch(batch_);
            if (!retired.ok()) {
                uncertain_ = true;
                outcome_ = Status::BatchCleanupDeferred("scatter retirement");
                return;
            }
            batch_ = 0;
            reject(s);
        } else {
            submitted_ = true;
        }
    }
    bool uncertain() const { return uncertain_; }
    Status wait(std::chrono::steady_clock::time_point end) {
        if (!submitted_ && batch_ && !uncertain_) {
            (void)orbitte_abandon_v1(handle_, batch_);
            batch_ = 0;
        }
        if (uncertain_ || !batch_)
            return outcome_;
        unsigned polls = 0;
        for (;;) {
            tent::TransferStatus overall{};
            auto progress = engine_->progressBatch(batch_, overall);
            if (!progress.ok()) {
                uncertain_ = true;
                return outcome_ = Status::BatchCleanupDeferred(progress.ToString());
            }
            if (orbitte_tasks_v1(handle_, batch_, snapshot_.data(), snapshot_.size()) != 0) {
                uncertain_ = true;
                return outcome_ = Status::BatchCleanupDeferred("cannot inspect scatter owners");
            }
            for (size_t i = 0; i < snapshot_.size(); ++i) {
                const auto &task = snapshot_[i];
                if (delivered_[i] || task.status.status == STATUS_PENDING ||
                    task.status.status == STATUS_WAITING)
                    continue;
                if (task.status.status != STATUS_COMPLETED && !abort_requested_) {
                    abort_requested_ = true;
                    for (size_t j = 0; j < snapshot_.size(); ++j) {
                        if (snapshot_[j].status.status == STATUS_PENDING ||
                            snapshot_[j].status.status == STATUS_WAITING)
                            (void)engine_->cancelTransfer(batch_, j);
                    }
                }
                if (task.unquiesced && !task.drained) {
                    uncertain_ = true;
                    return outcome_ =
                               Status::BatchCleanupDeferred("scatter physical drain unconfirmed");
                }
                if (task.drained) {
                    delivered_[i] = true;
                    complete(tasks_[i].range, tasks_[i].index,
                             task.status.status == STATUS_COMPLETED
                                 ? Status::OK()
                                 : Status::Socket("scatter transfer failed"));
                }
            }
            if (overall.s != tent::INITIAL && overall.s != tent::PENDING &&
                std::all_of(delivered_.begin(), delivered_.end(), [](bool v) { return v; })) {
                auto rc = orbitte_reap_v1(handle_, batch_);
                if (!rc) {
                    batch_ = 0;
                    tasks_.clear();
                    close();
                    return outcome_;
                }
                if (rc != -2) {
                    uncertain_ = true;
                    return outcome_ =
                               Status::BatchCleanupDeferred("scatter batch drain unconfirmed");
                }
            }
            if (std::chrono::steady_clock::now() >= end)
                return Status::Clock("scatter wait timed out");
            tent::waitBeforeNextPoll(polls);
            if (polls < 136)
                ++polls;
        }
    }
    ~Impl() {
        if (engine_ && !peers_.empty())
            close();
    }
};

TransferEngine::ScatterTransferOperation::ScatterTransferOperation(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
TransferEngine::ScatterTransferOperation::ScatterTransferOperation(
    ScatterTransferOperation &&) noexcept = default;
TransferEngine::ScatterTransferOperation &
TransferEngine::ScatterTransferOperation::operator=(ScatterTransferOperation &&other) noexcept {
    if (this != &other) {
        ScatterTransferOperation old(std::move(impl_));
        impl_ = std::move(other.impl_);
    }
    return *this;
}
TransferEngine::ScatterTransferOperation::~ScatterTransferOperation() {
    if (!impl_)
        return;
    // A retained Rust batch charges this operation. Its callback captures may
    // themselves own the C buffers. Preserve both across uncertain retirement.
    try {
        (void)wait();
        if (!impl_->uncertain())
            return;
    } catch (...) {
    }
    (void)impl_.release();
}
Status TransferEngine::ScatterTransferOperation::wait() {
    return impl_ ? impl_->wait(std::chrono::steady_clock::time_point::max())
                 : Status::InvalidArgument("moved scatter operation");
}
Status TransferEngine::ScatterTransferOperation::waitFor(std::chrono::nanoseconds timeout) {
    using Clock = std::chrono::steady_clock;
    auto now = Clock::now();
    auto end = timeout <= std::chrono::nanoseconds::zero() ? now
               : timeout >= Clock::time_point::max() - now ? Clock::time_point::max()
                                                           : now + timeout;
    return impl_ ? impl_->wait(end) : Status::InvalidArgument("moved scatter operation");
}
TransferEngine::ScatterTransferOperation
TransferEngine::submitScatter(const std::vector<ScatterTransferRange> &ranges) {
    auto e = getTentEngine();
    ScatterTransferOperation op(
        std::make_unique<ScatterTransferOperation::Impl>(e, e ? e->owner_ : nullptr));
    op.impl_->start(ranges);
    return op;
}
Status TransferEngine::transferScatter(const std::vector<ScatterTransferRange> &r) {
    auto op = submitScatter(r);
    return op.wait();
}
} // namespace mooncake
