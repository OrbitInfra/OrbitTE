/* Run on two installed hosts. The requester and source each verify the guards. */
#define _POSIX_C_SOURCE 200809L
#include <tent/transfer_engine.h>
#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum { WIDTH = 8194 };
static void ok(int rc) {
    if (rc) {
        fprintf(stderr, "libtent returned %d\n", rc);
        exit(1);
    }
}
static void tick(void) {
    struct timespec delay = {0, 1000000};
    nanosleep(&delay, NULL);
}
static void move(tent_engine_t e, int op, void *local, tent_segment_id_t id, uint64_t remote,
                 size_t count, int hint) {
    tent_request_t request = {.opcode = op,
                              .source = local,
                              .target_id = id,
                              .target_offset = remote,
                              .length = count,
                              .transport_hint = hint};
    tent_batch_id_t batch = tent_allocate_batch(e, 1);
    assert(batch != 0);
    ok(tent_submit(e, batch, &request, 1));
    for (int i = 0;; ++i) {
        tent_status_t status = {0};
        ok(tent_overall_status(e, batch, &status));
        if (status.status == STATUS_COMPLETED) {
            assert(status.transferred_bytes == count);
            break;
        }
        assert(i < 30000 && (status.status == STATUS_PENDING || status.status == STATUS_WAITING));
        tick();
    }
    for (int i = 0;; ++i) {
        int rc = tent_free_batch(e, batch);
        if (!rc)
            return;
        assert(i < 30000 && rc == -2);
        tick();
    }
}
static void rails(tent_engine_t e) {
    struct {
        tent_nic_load_stat_t one;
        unsigned char guard[16];
    } short_out = {0};
    memset(short_out.guard, 0xab, sizeof short_out.guard);
    size_t n = 1;
    ok(tent_get_nic_load_stats(e, &short_out.one, &n));
    assert(n >= 2 && n <= 4096);
    for (size_t i = 0; i < sizeof short_out.guard; ++i)
        assert(short_out.guard[i] == 0xab);
    tent_nic_load_stat_t *list = calloc(n, sizeof *list);
    assert(list);
    size_t count = n;
    ok(tent_get_nic_load_stats(e, list, &count));
    assert(count == n);
    for (size_t i = 0; i < n; ++i) {
        assert(list[i].device_name[0] &&
               memchr(list[i].device_name, 0, sizeof list[i].device_name));
        printf("NIC %s inflight=%" PRIu64 " rate=%" PRIu64 "\n", list[i].device_name,
               (uint64_t)list[i].inflight_bytes, (uint64_t)list[i].ewma_bandwidth_bps);
    }
    free(list);
}
static unsigned char pattern(size_t i, int salt) { return (unsigned char)(i * 71u + salt); }
int main(int argc, char **argv) {
    assert(argc == 4 || argc == 5);
    int source = !strcmp(argv[1], "source");
    assert(source ? argc == 4 : (!strcmp(argv[1], "requester") && argc == 5));
    int route = !strcmp(argv[2], "rdma") ? TRANSPORT_RDMA : TRANSPORT_TCP;
    assert(route == TRANSPORT_RDMA || !strcmp(argv[2], "tcp"));
    tent_set_config("max_failover_attempts", "0");
    tent_engine_t e = tent_create_engine();
    assert(tent_available(e));
    unsigned char scratch[WIDTH];
    /* Keep the CPU scratch private when probing the source's published buffer. */
    ok(tent_register_memory_with_perm(e, scratch, WIDTH, PERM_LOCAL_READ_WRITE));
    void *backing = NULL;
    ok(tent_allocate_memory(e, &backing, WIDTH, argv[3]));
    ok(tent_register_memory(e, backing, WIDTH));
    uint64_t address = (uintptr_t)backing;
    for (size_t i = 0; i < WIDTH; ++i)
        scratch[i] = source ? pattern(i, 1) : 0xcc;
    move(e, OPCODE_WRITE, scratch, LOCAL_SEGMENT_ID, address, WIDTH, TRANSPORT_TCP);
    if (source) {
        char name[256] = {0};
        ok(tent_segment_name(e, name, sizeof name));
        printf("source %s\n", name);
        fflush(stdout);
        for (int i = 0;; ++i) {
            tent_notifi_info list = {0};
            ok(tent_recv_notifs(e, &list));
            int finished = list.num_records == 1 && !strcmp(list.records[0].name, "verify") &&
                           !strcmp(list.records[0].msg, "complete");
            tent_free_notifs(&list);
            if (finished)
                break;
            assert(i < 120000);
            tick();
        }
        move(e, OPCODE_READ, scratch, LOCAL_SEGMENT_ID, address, WIDTH, TRANSPORT_TCP);
        for (size_t i = 0; i < WIDTH; ++i)
            assert(scratch[i] == pattern(i, i == 0 || i == WIDTH - 1 ? 1 : 2));
    } else {
        tent_segment_id_t peer;
        ok(tent_open_segment(e, &peer, argv[4]));
        tent_segment_info_t info = {0};
        ok(tent_get_segment_info(e, peer, &info));
        assert(info.num_buffers == 1 && info.buffers[0].length == WIDTH);
        uint64_t remote = info.buffers[0].base;
        if (strncmp(argv[3], "cuda:", 5) == 0)
            assert(strncmp(info.buffers[0].location, "cuda:", 5) == 0);
        tent_free_segment_info(&info);
        move(e, OPCODE_READ, (unsigned char *)backing + 1, peer, remote + 1, WIDTH - 2, route);
        move(e, OPCODE_READ, scratch, LOCAL_SEGMENT_ID, address, WIDTH, TRANSPORT_TCP);
        for (size_t i = 0; i < WIDTH; ++i) {
            assert(scratch[i] == (i == 0 || i == WIDTH - 1 ? 0xcc : pattern(i, 1)));
            scratch[i] = pattern(i, 2);
        }
        move(e, OPCODE_WRITE, scratch, LOCAL_SEGMENT_ID, address, WIDTH, TRANSPORT_TCP);
        move(e, OPCODE_WRITE, (unsigned char *)backing + 1, peer, remote + 1, WIDTH - 2, route);
        ok(tent_send_notifs(e, peer, "verify", "complete"));
        ok(tent_close_segment(e, peer));
    }
    if (route == TRANSPORT_RDMA)
        rails(e);
    ok(tent_unregister_memory(e, backing, WIDTH));
    ok(tent_free_memory(e, backing));
    ok(tent_unregister_memory(e, scratch, WIDTH));
    tent_destroy_engine(e);
    puts("verified");
    return 0;
}
