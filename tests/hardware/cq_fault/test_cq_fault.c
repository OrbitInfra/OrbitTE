/* Local callback controls only. Native receipts require the two-host gate. */
#include "cq_fault.c"
#include <assert.h>

static int posts, polls, poll_code;
static struct ibv_wc completions[3];

static int fake_post(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad) {
    (void)qp; (void)wr; (void)bad;
    ++posts;
    return 0;
}
static int fake_poll(struct ibv_cq *cq, int capacity, struct ibv_wc *wc) {
    (void)cq;
    ++polls;
    assert(capacity >= 3);
    if (poll_code > 0) memcpy(wc, completions, (size_t)poll_code * sizeof(*wc));
    return poll_code;
}

int main(void) {
    struct ibv_context context[2] = {0};
    for (int i = 0; i < 2; ++i) {
        context[i].ops.poll_cq = fake_poll;
        context[i].ops.post_send = fake_post;
        attach(&context[i]);
    }
    struct snapshot output;
    assert(sizeof(output) == 72);
    assert(orbitte_cq_fault(1, 0, 2, 1, &output) == 0);
    assert(orbitte_cq_fault(1, 0, 2, 1, &output) == EINVAL);
    assert(orbitte_cq_fault(2, 0, 0, 0, &output) == EINVAL);
    struct ibv_cq cq = {.context = &context[0]};
    struct ibv_qp qp = {.context = &context[0], .send_cq = &cq, .qp_num = 7};
    struct ibv_sge sge = {.addr = 1024, .length = 64};
    struct ibv_send_wr wr = {.wr_id = 9, .sg_list = &sge, .num_sge = 1, .opcode = IBV_WR_RDMA_WRITE};
    struct ibv_send_wr *bad = NULL;
    assert(context[0].ops.post_send(&qp, &wr, &bad) == 0 && posts == 1 && bad == NULL);
    completions[0] = (struct ibv_wc){.wr_id = 3, .status = IBV_WC_SUCCESS, .opcode = IBV_WC_SEND};
    completions[1] = (struct ibv_wc){.wr_id = 9, .qp_num = 7, .status = IBV_WC_SUCCESS, .opcode = IBV_WC_RDMA_WRITE};
    completions[2] = (struct ibv_wc){.wr_id = 5, .status = IBV_WC_WR_FLUSH_ERR, .opcode = IBV_WC_RDMA_WRITE};
    struct ibv_wc received[3] = {0};
    poll_code = 3;
    assert(context[0].ops.poll_cq(&cq, 3, received) == 2 && polls == 1);
    assert(received[0].wr_id == 3 && received[1].wr_id == 5 && received[1].status == IBV_WC_WR_FLUSH_ERR);
    poll_code = -5;
    assert(context[0].ops.poll_cq(&cq, 3, received) == -5 && polls == 2);
    assert(orbitte_cq_fault(0, 0, 0, 0, &output) == 0 && output.posts == 1 && output.lost == 1 && output.errors == 0);
    assert(orbitte_cq_fault(2, 0, 0, 0, &output) == 0 && output.armed == 0);
    poll_code = 3;
    assert(context[0].ops.poll_cq(&cq, 3, received) == 3 && received[1].wr_id == 9);
    assert(orbitte_cq_fault(1, 1, 1, 1, &output) == 0 && output.posts == 0 && output.lost == 0);
    assert(context[0].ops.poll_cq(&cq, 3, received) == 3);
    puts("CQ fault controls passed: native callbacks once, selective loss, errors/pass-through, generation fences");
    return 0;
}
