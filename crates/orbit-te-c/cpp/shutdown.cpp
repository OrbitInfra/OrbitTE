// The classic opt-in signal hook owns one watcher per process. Signal handlers
// only write a byte. The watcher closes admission through live Rust owners.
#include "shutdown.h"
#include "tent/cpp_v1.h"
#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <fcntl.h>
#include <mutex>
#include <pthread.h>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unistd.h>
namespace mooncake::detail {
namespace {
struct Watcher {
    struct Peer {
        std::weak_ptr<tent::TransferEngine> owner;
        tent_engine_t handle;
    };
    std::vector<Peer> peers;
    int pipe[2]{-1, -1};
    struct sigaction old_term{}, old_int{};
    std::thread worker;
};
std::mutex registry;
Watcher *active = nullptr;
volatile std::sig_atomic_t signal_fd = -1;
void signal_handler(int sig) {
    int saved = errno;
    unsigned char byte = static_cast<unsigned char>(sig);
    const auto fd = signal_fd;
    if (fd >= 0) {
        ssize_t rc;
        do {
            rc = write(fd, &byte, 1);
        } while (rc < 0 && errno == EINTR);
        if (rc < 0 && errno != EAGAIN)
            _Exit(128 + sig);
    }
    errno = saved;
}
void prepare_fork() { registry.lock(); }
void parent_fork() { registry.unlock(); }
void child_fork() {
    if (active) {
        sigaction(SIGINT, &active->old_int, nullptr);
        sigaction(SIGTERM, &active->old_term, nullptr);
        signal_fd = -1;
        close(active->pipe[1]);
        close(active->pipe[0]);
        // Other threads vanished in this child; never join or destroy their objects.
        active = nullptr;
    }
    registry.unlock();
}
void drain(std::vector<Watcher::Peer> &peers) {
    for (const auto &p : peers)
        if (auto live = p.owner.lock())
            (void)orbitte_close_v1(p.handle);
}
void stop() {
    std::unique_ptr<Watcher> w;
    {
        std::lock_guard lock(registry);
        if (!active)
            return;
        w.reset(active);
        active = nullptr;
        signal_fd = -1;
        sigaction(SIGINT, &w->old_int, nullptr);
        sigaction(SIGTERM, &w->old_term, nullptr);
        close(w->pipe[1]);
    }
    if (w->worker.joinable())
        w->worker.join();
    close(w->pipe[0]);
    drain(w->peers);
}
void watch(Watcher *w) {
    unsigned char byte = 0;
    ssize_t rc;
    do {
        rc = read(w->pipe[0], &byte, 1);
    } while (rc < 0 && errno == EINTR);
    if (rc == 0)
        return;
    if (rc != 1)
        _Exit(1);
    std::vector<Watcher::Peer> peers;
    {
        std::lock_guard lock(registry);
        if (active != w)
            return;
        peers.swap(w->peers);
    }
    drain(peers);
    _Exit(128 + byte);
}
void install() {
    static const bool hooks = [] {
        return pthread_atfork(prepare_fork, parent_fork, child_fork) == 0 && std::atexit(stop) == 0;
    }();
    if (!hooks)
        throw std::runtime_error("register graceful shutdown hooks");
    auto w = std::make_unique<Watcher>();
    if (pipe2(w->pipe, O_CLOEXEC) < 0)
        throw std::system_error(errno, std::generic_category(), "signal pipe");
    bool term = false, interrupt = false;
    try {
        if (fcntl(w->pipe[1], F_SETFL, O_NONBLOCK) < 0)
            throw std::system_error(errno, std::generic_category(), "signal writer");
        struct sigaction sa{};
        sa.sa_handler = signal_handler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_RESTART;
        signal_fd = w->pipe[1];
        if (!(term = sigaction(SIGTERM, &sa, &w->old_term) == 0) ||
            !(interrupt = sigaction(SIGINT, &sa, &w->old_int) == 0))
            throw std::system_error(errno, std::generic_category(), "signal handler");
        w->worker = std::thread(watch, w.get());
        active = w.release();
    } catch (...) {
        signal_fd = -1;
        if (term)
            sigaction(SIGTERM, &w->old_term, nullptr);
        if (interrupt)
            sigaction(SIGINT, &w->old_int, nullptr);
        close(w->pipe[1]);
        close(w->pipe[0]);
        throw;
    }
}
} // namespace
void on_shutdown(const std::shared_ptr<tent::TransferEngine> &owner, tent_engine_t handle) {
    std::lock_guard lock(registry);
    if (!active)
        install();
    auto &peers = active->peers;
    peers.erase(
        std::remove_if(peers.begin(), peers.end(), [](const auto &p) { return p.owner.expired(); }),
        peers.end());
    if (!handle ||
        std::any_of(peers.begin(), peers.end(), [&](const auto &p) { return p.handle == handle; }))
        return;
    // The C handle table bounds process-wide installed engines.
    if (peers.size() >= 1024)
        throw std::length_error("graceful shutdown engine capacity");
    peers.push_back({owner, handle});
}
} // namespace mooncake::detail
