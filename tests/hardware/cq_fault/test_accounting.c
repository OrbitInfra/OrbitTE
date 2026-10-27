/* Callback controls; physical contention is established only by native logs. */
#include "cq_fault.c"
#include <assert.h>

static int post_calls, poll_calls, poll_count;
static struct ibv_wc produced[3];
static int fake_post(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad) {
    (void)qp; (void)wr; (void)bad; ++post_calls; return 0;
}
static int fake_poll(struct ibv_cq *cq, int capacity, struct ibv_wc *wc) {
    (void)cq; assert(capacity >= 3); ++poll_calls;
    if (poll_count > 0) memcpy(wc, produced, (size_t)poll_count * sizeof(*wc));
    return poll_count;
}
int main(void) {
    struct ibv_context context[2] = {0};
    struct ibv_cq cq[2] = {0};
    struct ibv_qp qp[2] = {0};
    for (int i = 0; i < 2; ++i) {
        context[i].ops.poll_cq = fake_poll; context[i].ops.post_send = fake_post;
        attach(&context[i]); cq[i].context = &context[i];
        qp[i] = (struct ibv_qp){.context = &context[i], .send_cq = &cq[i], .qp_num = (unsigned)i + 7};
    }
    struct accounting_snapshot output;
    struct snapshot loss;
    assert(sizeof(output) == 80);
    for (unsigned op = 1; op <= 2; ++op) {
        assert(orbitte_rail_accounting(1, op, op, 16, &output) == 0);
        assert(orbitte_rail_accounting(1, op, op, 16, &output) == EINVAL);
        assert(orbitte_cq_fault(1, 0, op, 1, &loss) == EINVAL);
        assert(orbitte_rail_accounting(2, 0, 0, 0, &output) == EINVAL);
        for (int rail = 0; rail < 2; ++rail) for (int i = 0; i < 8; ++i) {
            struct ibv_sge sge = {.addr = 1024, .length = 64};
            struct ibv_send_wr wr = {.wr_id = (unsigned)i + 1, .sg_list = &sge, .num_sge = 1,
                .opcode = op == 1 ? IBV_WR_RDMA_READ : IBV_WR_RDMA_WRITE};
            struct ibv_send_wr *bad = NULL;
            assert(post(&qp[rail], &wr, &bad) == 0 && !bad);
        }
        assert(orbitte_rail_accounting(0, 0, 0, 0, &output) == 0);
        assert(output.pending == 16 && output.peak == 16 && output.rail_peak[0] == 8 && output.rail_peak[1] == 8);
        for (int rail = 0; rail < 2; ++rail) for (int i = 0; i < 8; ++i) {
            produced[0] = (struct ibv_wc){.wr_id = 999, .status = IBV_WC_SUCCESS, .opcode = IBV_WC_SEND};
            produced[1] = (struct ibv_wc){.wr_id = (unsigned)i + 1, .qp_num = qp[rail].qp_num,
                .status = IBV_WC_SUCCESS, .opcode = op == 1 ? IBV_WC_RDMA_READ : IBV_WC_RDMA_WRITE, .byte_len = 64};
            produced[2] = (struct ibv_wc){.wr_id = 1000, .status = IBV_WC_WR_FLUSH_ERR};
            struct ibv_wc received[3] = {0};
            poll_count = 3;
            assert(poll(&cq[rail], 3, received) == 3 && memcmp(received, produced, sizeof(produced)) == 0);
        }
        assert(orbitte_rail_accounting(2, 0, 0, 0, &output) == 0);
        assert(output.completed == 16 && output.bytes == 1024 && output.pending == 0 && !output.active);
    }
    assert(post_calls == 32 && poll_calls == 32);
    struct ibv_wc received[3]; poll_count = -5;
    assert(poll(&cq[0], 3, received) == -5 && poll_calls == 33);
    assert(orbitte_rail_accounting(1, 3, 1, 1, &output) == 0);
    /* A CQE without an accepted WR must fail closed, while passing it through. */
    poll_count = 3;
    assert(poll(&cq[0], 3, received) == 3);
    assert(orbitte_rail_accounting(0, 0, 0, 0, &output) == EINVAL);
    puts("Rail accounting controls passed: native callbacks once, untouched CQEs, bounded debt, exact joins");
    return 0;
}
