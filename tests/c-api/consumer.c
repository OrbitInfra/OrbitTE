/* Installed public API consumer. No OrbitTE internals or Rust linkage helpers. */
#define _POSIX_C_SOURCE 200809L
#include <tent/transfer_engine.h>
#include <assert.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define OK(x)                                                                                      \
    do {                                                                                           \
        int rc_ = (x);                                                                             \
        if (rc_) {                                                                                 \
            fprintf(stderr, "%s:%d %s returned %d\n", __FILE__, __LINE__, #x, rc_);                \
            abort();                                                                               \
        }                                                                                          \
    } while (0)

static void tick(void) {
    struct timespec t = {0, 1000000};
    nanosleep(&t, NULL);
}

static tent_status_t wait_batch(tent_engine_t e, tent_batch_id_t b) {
    for (int i = 0; i < 30000; ++i) {
        tent_status_t s = {0};
        OK(tent_overall_status(e, b, &s));
        if (s.status != STATUS_PENDING && s.status != STATUS_WAITING)
            return s;
        tick();
    }
    abort();
}

static void free_batch(tent_engine_t e, tent_batch_id_t b) {
    for (int i = 0; i < 30000; ++i) {
        int rc = tent_free_batch(e, b);
        if (!rc)
            return;
        assert(rc == -2);
        tick();
    }
    abort();
}

static tent_engine_t engine(void) {
    tent_engine_t e = tent_create_engine();
    assert(e && tent_available(e));
    return e;
}

static void *tls_worker(void *arg) {
    /* Constructor fields take precedence over MC_TENT_CONF and the hostname alias. */
    tent_load_config_from_file("{\"rpc_server_hostname\":\"127.0.0.3\",\"rpc_server_port\":0}");
    tent_set_config("rpc_server_threads", "0");
    tent_set_config("rpc_server_threads", "2");
    tent_engine_t e = engine();
    char host[64] = {0};
    uint16_t port = 0;
    OK(tent_rpc_server_addr_port(e, host, sizeof host, &port));
    assert(!strcmp(host, "127.0.0.3") && port != 0);
    tent_destroy_engine(e);
    /* Frozen wrapper parses its argument as text even if it names an existing file. */
    tent_load_config_from_file((const char *)arg);
    e = engine();
    OK(tent_rpc_server_addr_port(e, host, sizeof host, &port));
    assert(!strcmp(host, "127.0.0.2"));
    tent_destroy_engine(e);
    tent_set_config("rpc_server_port", "70000");
    e = tent_create_engine();
    assert(e && !tent_available(e));
    tent_destroy_engine(e);
    return NULL;
}

static void local_contracts(const char *filename, const char *file_uri) {
    assert(!tent_available(NULL));
    tent_destroy_engine(NULL);
    assert(tent_allocate_batch(NULL, 1) == UINT64_MAX);
    tent_free_segment_info(NULL);
    tent_free_notifs(NULL);
    pthread_t thread;
    tent_set_config("rpc_server_hostname", "127.0.0.1");
    assert(!pthread_create(&thread, NULL, tls_worker, (void *)filename));
    tent_engine_t e = engine();
    assert(!pthread_join(thread, NULL));
    char name[128] = {0}, host[64] = {0};
    uint16_t port = 0;
    OK(tent_segment_name(e, name, sizeof name));
    OK(tent_rpc_server_addr_port(e, host, sizeof host, &port));
    assert(!strcmp(host, "127.0.0.1") && port);
    char expected[128];
    snprintf(expected, sizeof expected, "%s:%u", host, (unsigned)port);
    assert(!strcmp(name, expected));
    char guard[3] = {'a', 'b', 'c'};
    OK(tent_segment_name(e, guard + 1, 1));
    assert(!memcmp(guard, "a1c", 3));
    OK(tent_rpc_server_addr_port(e, guard + 1, 1, &port));
    assert(!memcmp(guard, "a1c", 3));
    assert(tent_rpc_server_addr_port(e, host, sizeof host, NULL) == -1);
    assert(tent_segment_name(e, NULL, 8) == -1);
    tent_segment_id_t local = 42;
    OK(tent_open_segment(e, &local, ""));
    assert(local == LOCAL_SEGMENT_ID);
    OK(tent_open_segment(e, &local, name));
    assert(local == LOCAL_SEGMENT_ID);
    OK(tent_close_segment(e, local));
    assert(tent_open_segment(e, NULL, name) == -1 && tent_close_segment(e, UINT64_MAX) == -1);
    tent_segment_info_t info = {0};
    OK(tent_get_segment_info(e, local, &info));
    assert(info.type == TYPE_MEMORY && info.num_buffers == 0 && info.buffers == NULL);
    assert(tent_get_segment_info(e, local, NULL) == -1);
    tent_segment_id_t file = 0, again = 0;
    OK(tent_open_segment(e, &file, file_uri));
    OK(tent_open_segment(e, &again, file_uri));
    assert(file == again && file != local);
    OK(tent_get_segment_info(e, file, &info));
    assert(info.type == TYPE_FILE && info.num_buffers == 1);
    assert(info.buffers[0].base == 0 && info.buffers[0].length == 11 &&
           !strcmp(info.buffers[0].location, "*"));
    tent_free_segment_info(&info);
    assert(info.buffers != NULL);
    info.buffers = NULL;
    OK(tent_close_segment(e, file));
    assert(tent_close_segment(e, again) == -1);

    void *a = NULL, *b = NULL;
    unsigned char raw[2][16] = {{0}};
    assert(tent_allocate_memory(e, NULL, 16, "*") == -1 &&
           tent_allocate_memory(e, &a, 0, "*") == -1);
    assert(tent_allocate_memory(e, &a, 16, NULL) == -1);
    OK(tent_allocate_memory(e, &a, 16, "*"));
    tent_memory_options_t o = {0};
    o.permission = PERM_GLOBAL_READ_WRITE;
    o.transport_type = TRANSPORT_TCP;
    OK(tent_allocate_memory_ex(e, &b, 16, &o));
    memset(a, 0xa5, 16);
    memset(b, 0xb6, 16);
    OK(tent_register_memory(e, a, 16));
    OK(tent_register_memory_ex(e, b, 16, &o));
    assert(tent_free_memory(e, a) == -2);
    OK(tent_unregister_memory(e, a, 16));
    OK(tent_free_memory(e, a));
    OK(tent_unregister_memory(e, b, 0));
    OK(tent_free_memory(e, b));
    assert(tent_free_memory(e, b) == -1);
    void *ptrs[] = {raw[0], raw[1]};
    size_t sizes[] = {sizeof raw[0], sizeof raw[1]};
    strcpy(o.location, "cpu:42");
    assert(tent_register_memory_ex(e, raw[0], 16, &o) == -200);
    o.location[0] = 0;
    OK(tent_register_memory_batch(e, ptrs, sizes, 2, PERM_GLOBAL_READ_ONLY));
    OK(tent_get_segment_info(e, local, &info));
    assert(info.num_buffers == 2);
    tent_free_segment_info(&info);
    OK(tent_unregister_memory_batch(e, ptrs, NULL, 2));
    OK(tent_register_memory_batch_ex(e, ptrs, sizes, 2, &o));
    OK(tent_unregister_memory_batch(e, ptrs, sizes, 2));
    o.internal = 1;
    OK(tent_register_memory_ex(e, raw[0], 16, &o));
    OK(tent_get_segment_info(e, local, &info));
    assert(info.num_buffers == 0 && !info.buffers);
    OK(tent_unregister_memory(e, raw[0], 16));
    o.internal = 0;
    int excluded[] = {
        TRANSPORT_ASCEND_DIRECT, TRANSPORT_SUNRISE_LINK, TRANSPORT_TPU, TRANSPORT_UB,
        TRANSPORT_XPU,           TRANSPORT_HYLINK,       TRANSPORT_MLU, TRANSPORT_MEMFABRIC,
        TRANSPORT_FABRIC,        TRANSPORT_MPCOMM};
    for (size_t i = 0; i < sizeof excluded / sizeof *excluded; i++) {
        o.transport_type = excluded[i];
        assert(tent_register_memory_ex(e, raw[0], 16, &o) == -200);
        assert(tent_allocate_memory_ex(e, &a, 16, &o) == -200);
    }
    o.transport_type = 97;
    assert(tent_register_memory_ex(e, raw[0], 16, &o) == -1);
    assert(tent_register_memory_batch(e, NULL, sizes, 2, 2) == -1);
    assert(tent_register_memory_batch_ex(e, ptrs, NULL, 2, &o) == -1);
    assert(tent_unregister_memory_batch(e, NULL, NULL, 2) == -1);
    assert(tent_register_memory_with_perm(e, raw, 16, 31) == -1);
    assert(tent_register_memory(e, NULL, 16) == -1 && tent_register_memory(e, raw, 0) == -1);
    assert(tent_unregister_memory(e, raw, 16) == -3);
    tent_notifi_info inbox = {0};
    OK(tent_recv_notifs(e, &inbox));
    assert(!inbox.records && !inbox.num_records);
    OK(tent_send_notifs(e, local, "a", "one"));
    OK(tent_send_notifs(e, local, "b", "two"));
    OK(tent_recv_notifs(e, &inbox));
    assert(inbox.num_records == 2 && inbox.records[0].handle == 0);
    assert(!strcmp(inbox.records[0].name, "a") && !strcmp(inbox.records[1].msg, "two"));
    tent_free_notifs(&inbox);
    assert(!inbox.records && !inbox.num_records);
    tent_free_notifs(&inbox);
    assert(tent_recv_notifs(e, NULL) == -1 && tent_send_notifs(e, local, NULL, "m") == -1);
    tent_nic_load_stat_t nic = {0};
    nic.inflight_bytes = 73;
    size_t count = 0;
    OK(tent_get_nic_load_stats(e, &nic, &count));
    assert(count == 0 && nic.inflight_bytes == 73);
    assert(tent_get_nic_load_stats(e, NULL, &count) == -1);
    assert(tent_get_nic_load_stats(e, &nic, NULL) == -1);
    tent_status_t st;
    assert(tent_task_status(e, 0, 0, &st) == -1);
    assert(tent_allocate_batch(e, (size_t)-1) == 0);
    tent_batch_id_t id = tent_allocate_batch(e, 1);
    assert(id && id != UINT64_MAX);
    tent_request_t entries[2] = {{0}};
    assert(tent_submit(e, id, entries, 2) == -2 &&
           tent_submit_notif(e, id, entries, 2, "n", "m") == -2);
    assert(tent_submit(e, id, NULL, 0) == -1 && tent_cancel_task(e, id, 0) == -1);
    assert(tent_overall_status(e, id, NULL) == -1 &&
           tent_task_status_list(e, id, NULL, &count) == -1);
    OK(tent_overall_status(e, id, &st));
    assert(st.status == STATUS_COMPLETED && !st.transferred_bytes);
    OK(tent_free_batch(e, id));
    assert(tent_free_batch(e, id) == -1);
    tent_destroy_engine(e);
    assert(!tent_available(e));
    assert(tent_close_segment(e, 0) == -1);
}

static unsigned char rw[24], ro[13], hidden[7], private_bytes[9];

static void source(void) {
    tent_engine_t e = engine();
    memset(rw, '#', sizeof rw);
    memcpy(rw + 4, "abcdef", 6);
    memset(ro, 'R', sizeof ro);
    tent_memory_options_t tcp = {0};
    tcp.transport_type = TRANSPORT_TCP;
    tcp.permission = PERM_GLOBAL_READ_WRITE;
    OK(tent_register_memory_ex(e, rw, sizeof rw, &tcp));
    OK(tent_register_memory_with_perm(e, ro, sizeof ro, PERM_GLOBAL_READ_ONLY));
    OK(tent_register_memory_with_perm(e, private_bytes, sizeof private_bytes,
                                      PERM_LOCAL_READ_WRITE));
    tent_memory_options_t o = {0};
    o.permission = PERM_GLOBAL_READ_WRITE;
    o.internal = 1;
    OK(tent_register_memory_ex(e, hidden, sizeof hidden, &o));
    char name[128] = {0};
    OK(tent_segment_name(e, name, sizeof name));
    printf("%s\n", name);
    fflush(stdout);
    if (getchar() == 'v') {
        unsigned char want[sizeof rw];
        memset(want, '#', sizeof want);
        memcpy(want + 4, "aXYdef", 6);
        assert(!memcmp(rw, want, sizeof rw));
        for (size_t i = 0; i < sizeof ro; i++)
            assert(ro[i] == 'R');
        tent_notifi_info inbox = {0};
        OK(tent_recv_notifs(e, &inbox));
        assert(inbox.num_records == 2);
        assert(!strcmp(inbox.records[0].name, "committed") &&
               !strcmp(inbox.records[0].msg, "once"));
        assert(!strcmp(inbox.records[1].name, "lost-ack"));
        tent_free_notifs(&inbox);
    }
    OK(tent_unregister_memory(e, rw, sizeof rw));
    OK(tent_unregister_memory(e, ro, sizeof ro));
    OK(tent_unregister_memory(e, private_bytes, sizeof private_bytes));
    OK(tent_unregister_memory(e, hidden, sizeof hidden));
    tent_destroy_engine(e);
}
static void transfer(const char *source_name, const char *relay, const char *refused) {
    tent_engine_t e = engine();
    tent_segment_id_t s, proxy, down;
    OK(tent_open_segment(e, &down, refused));
    tent_segment_info_t info = {0};
    assert(tent_get_segment_info(e, down, &info) == -134);
    OK(tent_close_segment(e, down));
    OK(tent_open_segment(e, &s, source_name));
    OK(tent_get_segment_info(e, s, &info));
    assert(info.num_buffers == 2);
    uint64_t dst = 0, read_only = 0;
    for (int i = 0; i < info.num_buffers; i++) {
        if (info.buffers[i].length == sizeof rw)
            dst = info.buffers[i].base;
        if (info.buffers[i].length == sizeof ro)
            read_only = info.buffers[i].base;
        assert(!strcmp(info.buffers[i].location, "*"));
    }
    tent_free_segment_info(&info);
    assert(dst && read_only);
    unsigned char local[32];
    memset(local, '!', sizeof local);
    memcpy(local + 16, "XY", 2);
    OK(tent_register_memory(e, local, sizeof local));
    tent_batch_id_t id = tent_allocate_batch(e, 5);
    assert(id);
    tent_request_t requests[2] = {{0}};
    requests[0] = (tent_request_t){OPCODE_READ, local + 2, s, dst + 4, 6, 2, TRANSPORT_TCP};
    requests[1] =
        (tent_request_t){OPCODE_WRITE, local + 16, s, read_only + 1, 2, 1, TRANSPORT_UNSPEC};
    /* The second entry is read-only. No earlier READ may have been published. */
    assert(tent_submit(e, id, requests, 2) == -1);
    size_t n = 0;
    tent_status_t scalar = {0};
    OK(tent_task_status_list(e, id, &scalar, &n));
    assert(n == 0 && local[2] == '!');
    requests[1].target_offset = dst + 5;
    requests[1].transport_hint = TRANSPORT_UB;
    assert(tent_submit(e, id, requests, 2) == -200);
    requests[1].transport_hint = TRANSPORT_UNSPEC;
    OK(tent_submit_notif(e, id, requests, 2, "committed", "once"));
    scalar = wait_batch(e, id);
    assert(scalar.status == STATUS_COMPLETED && scalar.transferred_bytes == 8);
    OK(tent_task_status(e, id, 0, &scalar));
    assert(scalar.status == STATUS_COMPLETED && scalar.transferred_bytes == 6);
    tent_status_t list[2] = {{0}, {77, 88}};
    n = 1;
    OK(tent_task_status_list(e, id, list, &n));
    assert(n == 2 && list[0].transferred_bytes == 6 && list[1].status == 77 &&
           list[1].transferred_bytes == 88);
    OK(tent_cancel_task(e, id, 1));
    free_batch(e, id);
    unsigned char want[32];
    memset(want, '!', sizeof want);
    memcpy(want + 2, "abcdef", 6);
    memcpy(want + 16, "XY", 2);
    assert(!memcmp(local, want, sizeof local));
    OK(tent_open_segment(e, &proxy, relay));
    /* The relay consumes the real source's final enqueue receipt and drops it. */
    assert(tent_send_notifs(e, proxy, "lost-ack", "delivered") == -135);
    OK(tent_unregister_memory(e, local, sizeof local));
    OK(tent_close_segment(e, proxy));
    OK(tent_close_segment(e, s));
    tent_destroy_engine(e);
}
static void held(const char *name, int entered, int resume, int cut_read) {
    tent_engine_t e = engine();
    tent_segment_id_t segment;
    OK(tent_open_segment(e, &segment, name));
    tent_segment_info_t info = {0};
    OK(tent_get_segment_info(e, segment, &info));
    uint64_t address = 0;
    for (int i = 0; i < info.num_buffers; i++)
        if (info.buffers[i].length == sizeof rw)
            address = info.buffers[i].base;
    assert(address);
    tent_free_segment_info(&info);
    unsigned char dst[12], idle[16];
    memset(dst, '!', sizeof dst);
    memset(idle, '?', sizeof idle);
    void *ptrs[] = {dst, idle};
    size_t sizes[] = {sizeof dst, sizeof idle};
    OK(tent_register_memory_batch(e, ptrs, sizes, 2, PERM_LOCAL_READ_WRITE));
    tent_request_t reqs[] = {{OPCODE_READ, dst + 2, segment, address + 4, 6, 0, TRANSPORT_TCP},
                             {OPCODE_WRITE, dst, segment, address, 1, 0, TRANSPORT_TCP}};
    tent_batch_id_t id = tent_allocate_batch(e, 2);
    OK(tent_submit(e, id, reqs, 2));
    char gate = 0;
    assert(read(entered, &gate, 1) == 1 && gate == '!');
    OK(tent_cancel_task(e, id, 1));
    assert(tent_free_batch(e, id) == -2);
    assert(tent_unregister_memory_batch(e, ptrs, sizes, 2) == -2);
    assert(tent_unregister_memory_batch(e, ptrs, sizes, 2) == -2);
    assert(write(resume, "!", 1) == 1);
    tent_status_t whole = wait_batch(e, id);
    assert(whole.status == (cut_read ? STATUS_FAILED : STATUS_CANCELED));
    assert(whole.transferred_bytes == (cut_read ? 0u : 6u));
    tent_status_t first = {0};
    OK(tent_task_status(e, id, 0, &first));
    assert(first.status == (cut_read ? STATUS_FAILED : STATUS_COMPLETED));
    assert(first.transferred_bytes == whole.transferred_bytes);
    tent_status_t canceled = {0};
    OK(tent_task_status(e, id, 1, &canceled));
    assert(canceled.status == STATUS_CANCELED);
    free_batch(e, id);
    OK(tent_unregister_memory_batch(e, ptrs, sizes, 2));
    assert(!memcmp(dst, cut_read ? "!!!!!!!!!!!!" : "!!abcdef!!!!", sizeof dst));
    for (size_t i = 0; i < sizeof idle; i++)
        assert(idle[i] == '?');
    tent_destroy_engine(e);
}

static void epoch(const char *name) {
    tent_engine_t e = engine();
    tent_segment_id_t s;
    tent_segment_info_t info = {0};
    OK(tent_open_segment(e, &s, name));
    OK(tent_get_segment_info(e, s, &info));
    tent_free_segment_info(&info);
    puts("pinned");
    fflush(stdout);
    assert(getchar() == 'r');
    assert(tent_get_segment_info(e, s, &info) == -7);
    OK(tent_close_segment(e, s));
    OK(tent_open_segment(e, &s, name));
    OK(tent_get_segment_info(e, s, &info));
    tent_free_segment_info(&info);
    tent_destroy_engine(e);
}

static void shm_options(void) {
    tent_engine_t e = engine();
    tent_memory_options_t o = {0};
    o.permission = PERM_GLOBAL_READ_WRITE;
    o.transport_type = TRANSPORT_SHM;
    void *addr = NULL;
    OK(tent_allocate_memory_ex(e, &addr, 4096, &o));
    unsigned char *p = addr;
    memset(p, '!', 4096);
    memcpy(p + 8, "shm", 3);
    o.shm_offset = 8;
    OK(tent_register_memory_ex(e, p + 8, 8, &o));
    o.shm_offset = 16;
    assert(tent_register_memory_ex(e, p + 8, 8, &o) == -1);
    o.shm_offset = 8;
    strcpy(o.shm_path, "other-live-owner");
    assert(tent_register_memory_ex(e, p + 8, 8, &o) == -1);
    unsigned char dst[9] = "!!!!!!!!";
    OK(tent_register_memory(e, dst, sizeof dst));
    tent_request_t req = {OPCODE_READ, dst + 2, LOCAL_SEGMENT_ID, (uint64_t)(uintptr_t)(p + 8),
                          3,           0,       TRANSPORT_SHM};
    tent_batch_id_t batch = tent_allocate_batch(e, 1);
    OK(tent_submit(e, batch, &req, 1));
    assert(wait_batch(e, batch).status == STATUS_COMPLETED && !memcmp(dst, "!!shm!!!", 9));
    free_batch(e, batch);
    OK(tent_unregister_memory(e, dst, sizeof dst));
    OK(tent_unregister_memory(e, p + 8, 8));
    OK(tent_free_memory(e, p));
    tent_destroy_engine(e);
}

static void file_io(const char *uri) {
    tent_engine_t e = engine();
    tent_segment_id_t f;
    OK(tent_open_segment(e, &f, uri));
    char data[] = "!FILE!ZZZZZ!";
    OK(tent_register_memory(e, data, sizeof data));
    tent_request_t r[2] = {{OPCODE_WRITE, data + 1, f, 2, 4, 0, TRANSPORT_IOURING},
                           {OPCODE_READ, data + 7, f, 2, 4, 0, TRANSPORT_IOURING}};
    tent_batch_id_t batch = tent_allocate_batch(e, 2);
    OK(tent_submit(e, batch, r, 2));
    tent_status_t st = wait_batch(e, batch);
    assert(st.status == STATUS_COMPLETED && st.transferred_bytes == 8);
    assert(!strcmp(data, "!FILE!ZFILE!"));
    free_batch(e, batch);
    OK(tent_close_segment(e, f));
    OK(tent_unregister_memory(e, data, sizeof data));
    tent_destroy_engine(e);
}
int main(int argc, char **argv) {
    if (argc == 2 && !strcmp(argv[1], "source"))
        source();
    else if (argc == 4 && !strcmp(argv[1], "local"))
        local_contracts(argv[2], argv[3]);
    else if (argc == 5 && !strcmp(argv[1], "transfer"))
        transfer(argv[2], argv[3], argv[4]);
    else if (argc == 5 && (!strcmp(argv[1], "held") || !strcmp(argv[1], "cut-read")))
        held(argv[2], atoi(argv[3]), atoi(argv[4]), !strcmp(argv[1], "cut-read"));
    else if (argc == 3 && !strcmp(argv[1], "epoch"))
        epoch(argv[2]);
    else if (argc == 2 && !strcmp(argv[1], "shm"))
        shm_options();
    else if (argc == 3 && !strcmp(argv[1], "file"))
        file_io(argv[2]);
    else
        return 2;
    return 0;
}
