use super::*;
use crate::transport::rdma::{RdmaTransport, tests::runtime};
use std::time::{Duration, Instant};

fn fixture(limit: usize) -> (RdmaTransport, Arc<RdmaContext>, ReceiveQueue) {
    let runtime = runtime("ok");
    let device = runtime.device().unwrap();
    let queue = ReceiveQueue::new(limit, limit, Arc::clone(&runtime.usage)).unwrap();
    (runtime, device, queue)
}

fn work(index: u8) -> Arc<ReceiveWork> {
    let mut binding = [index; COMPLETION_MESSAGE_SIZE];
    binding[COMPLETION_MESSAGE_SIZE - 1] = 0;
    ReceiveWork::new(
        128 * u64::from(index) + 1,
        binding,
        0x0102_0300 + u32::from(index),
        Instant::now() + Duration::from_secs(3),
    )
}

// The CPU contract never submits native DMA; handles.qp selects a post receipt.
unsafe extern "C" fn post(
    handles: *mut sys::ConnectionHandles,
    _: *const sys::MemoryHandles,
    _: u64,
    _: u64,
    _: usize,
    result: *mut sys::PostResult,
) -> i32 {
    let mode = unsafe { (*handles).qp as usize };
    unsafe {
        *result = sys::PostResult {
            accepted: usize::from(mode == 0),
            uncertain: mode == 2,
        };
    }
    if mode == 0 { 0 } else { libc::ENOMEM }
}

fn prepare(queue: &mut ReceiveQueue, work: &Arc<ReceiveWork>, device: &RdmaContext) -> Result<()> {
    let credits = ReceiveCredits::acquire(Arc::clone(&queue.usage), 8, 1)?;
    queue.prepare(Arc::clone(work), credits, device, 8, std::ptr::null_mut())
}

fn posted(
    queue: &mut ReceiveQueue,
    work: &ReceiveWork,
    verbs: &sys::Verbs,
    mode: usize,
    capacity: usize,
) -> Result<()> {
    let verbs = sys::Verbs {
        post_receive: post,
        ..*verbs
    };
    queue.post(
        work.id,
        capacity,
        &mut sys::ConnectionHandles {
            qp: mode as *mut _,
            ..Default::default()
        },
        &verbs,
    )
}

fn delivered(queue: &mut ReceiveQueue, slot: u64, target: &ReceiveWork) -> sys::Completion {
    let message = &mut queue
        .slots
        .iter_mut()
        .find(|entry| entry.id == slot)
        .unwrap()
        .message;
    // The stub above never starts DMA; this models bytes written before a CQE.
    message.memory.bytes_mut().copy_from_slice(&target.expected);
    sys::Completion {
        id: slot,
        opcode: 128,
        length: COMPLETION_MESSAGE_SIZE as u32,
        flags: sys::WITH_INVALIDATE,
        invalidated_rkey: target.rkey,
        ..Default::default()
    }
}

// The CQ lane commits logical completion after revoking the named grant authority.
fn acknowledge(queue: &mut ReceiveQueue, completion: sys::Completion) -> Result<Arc<ReceiveWork>> {
    let receipt = queue.complete(completion)?;
    receipt.mark_completed();
    Ok(receipt)
}

fn close(runtime: RdmaTransport, device: Arc<RdmaContext>, mut queue: ReceiveQueue) {
    queue.quiesce();
    queue.close(runtime.verbs).unwrap();
    drop((queue, device));
    runtime
        .close(Instant::now() + Duration::from_secs(3))
        .unwrap();
    let usage = runtime.stats().unwrap();
    assert_eq!(
        (
            usage.receive_work_requests,
            usage.receive_credits,
            usage.message_bytes,
            usage.active_message_mrs
        ),
        (0, 0, 0, 0)
    );
}

#[test]
fn reversed_grant_messages_release_only_the_consumed_slot_and_named_authority() {
    let (runtime, device, mut queue) = fixture(2);
    let first = work(1);
    let second = work(2);
    for work in [&first, &second] {
        prepare(&mut queue, work, &device).unwrap();
        posted(&mut queue, work, runtime.verbs, 0, 2).unwrap();
    }
    let completion = delivered(&mut queue, first.id, &second);
    let receipt = acknowledge(&mut queue, completion).unwrap();
    assert!(Arc::ptr_eq(&receipt, &second));
    assert!(second.completed() && !first.completed());
    queue.release(&second, runtime.verbs).unwrap();
    drop((receipt, second));
    assert_eq!(queue.outstanding(), 1);
    assert_eq!(queue.memory_usage(), (1, COMPLETION_MESSAGE_SIZE));
    assert_eq!(queue.credited(), 1);
    assert_eq!(
        queue.slots[0].id, 257,
        "completed grant's donated slot still belongs to the QP"
    );
    let third = work(3);
    prepare(&mut queue, &third, &device).unwrap();
    let completion = delivered(&mut queue, 257, &first);
    assert!(Arc::ptr_eq(
        &acknowledge(&mut queue, completion).unwrap(),
        &first
    ));
    queue.release(&first, runtime.verbs).unwrap();
    assert_eq!(
        queue.memory_usage(),
        (1, COMPLETION_MESSAGE_SIZE),
        "releasing a completed grant must preserve another prepared slot"
    );
    assert_eq!(queue.credited(), 1);
    posted(&mut queue, &third, runtime.verbs, 0, 2).unwrap();
    let completion = delivered(&mut queue, third.id, &third);
    assert!(Arc::ptr_eq(
        &acknowledge(&mut queue, completion).unwrap(),
        &third
    ));
    queue.release(&third, runtime.verbs).unwrap();
    assert_eq!(queue.memory_usage(), (0, 0));
    close(runtime, device, queue);
}

#[test]
fn receive_identity_flags_binding_and_key_never_pay_foreign_debt() {
    for case in [
        "valid",
        "duplicate",
        "old_wr",
        "opcode",
        "length",
        "flags",
        "extra_flags",
        "key",
        "binding",
        "unposted",
        "wc_error",
    ] {
        let (runtime, device, mut queue) = fixture(2);
        let first = work(1);
        let second = work(2);
        prepare(&mut queue, &first, &device).unwrap();
        prepare(&mut queue, &second, &device).unwrap();
        posted(&mut queue, &first, runtime.verbs, 0, 2).unwrap();
        if case != "unposted" {
            posted(&mut queue, &second, runtime.verbs, 0, 2).unwrap();
        }
        let mut completion = delivered(&mut queue, first.id, &second);
        match case {
            "old_wr" => completion.id += 1024,
            "opcode" => completion.opcode = 129,
            "length" => completion.length -= 1,
            "flags" => completion.flags = 0,
            "extra_flags" => completion.flags |= 0x8000_0000,
            "key" => completion.invalidated_rkey = first.rkey,
            "binding" => queue.slots[0].message.memory.bytes_mut()[0] ^= 1,
            "wc_error" => completion.status = 5,
            _ => {}
        }
        let result = acknowledge(&mut queue, completion);
        let success = matches!(case, "valid" | "duplicate");
        assert_eq!(result.is_ok(), success, "{case}");
        assert_eq!(second.completed(), success, "{case}");
        assert!(!first.completed(), "{case}");
        let expected_debt = if success || matches!(case, "unposted" | "wc_error") {
            1
        } else {
            2
        };
        assert_eq!(queue.outstanding(), expected_debt, "{case}");
        if case == "duplicate" {
            let duplicate = delivered(&mut queue, first.id, &second);
            assert!(queue.complete(duplicate).is_err());
            let repeated_message = delivered(&mut queue, second.id, &second);
            assert!(queue.complete(repeated_message).is_err());
            assert_eq!(queue.outstanding(), 1);
        }
        close(runtime, device, queue);
    }
}

#[test]
fn bounded_slots_keep_uncertain_posts_and_dropped_receipts_until_native_drain() {
    for mode in [0, 1, 2] {
        let (runtime, device, mut queue) = fixture(2);
        let first = work(1);
        let second = work(2);
        prepare(&mut queue, &first, &device).unwrap();
        assert!(prepare(&mut queue, &first, &device).is_err());
        let duplicate_key = ReceiveWork::new(
            385,
            [3; COMPLETION_MESSAGE_SIZE],
            first.rkey,
            first.deadline,
        );
        assert!(prepare(&mut queue, &duplicate_key, &device).is_err());
        prepare(&mut queue, &second, &device).unwrap();
        assert!(matches!(
            prepare(&mut queue, &work(3), &device),
            Err(Error::ResourceExhausted)
        ));
        posted(&mut queue, &first, runtime.verbs, 0, 1).unwrap();
        assert!(matches!(
            posted(&mut queue, &second, runtime.verbs, 0, 1),
            Err(Error::ResourceExhausted)
        ));
        assert_eq!(
            posted(&mut queue, &second, runtime.verbs, mode, 2).is_ok(),
            mode == 0
        );
        drop((first, second));
        let expected = if mode == 1 { 1 } else { 2 };
        assert_eq!(queue.outstanding(), expected);
        assert_eq!(
            queue.credited(),
            2,
            "both prepared MRs retain admission until cleanup"
        );
        assert_eq!(queue.memory_usage(), (2, 2 * COMPLETION_MESSAGE_SIZE));
        assert!(queue.close(runtime.verbs).is_err());
        assert_eq!(queue.outstanding(), expected);
        assert_eq!(queue.memory_usage(), (2, 2 * COMPLETION_MESSAGE_SIZE));
        close(runtime, device, queue);
    }
}

impl ReceiveQueue {
    pub(in crate::transport::rdma) fn credited(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.credits.is_some())
            .count()
    }
}

#[test]
fn mr_authority_requires_plain_send_and_exact_binding_without_claiming_invalidation() {
    for case in ["valid", "invalidation", "binding", "profile"] {
        let (runtime, device, mut queue) = fixture(1);
        let mut binding = [0x55; COMPLETION_MESSAGE_SIZE];
        binding[COMPLETION_MESSAGE_SIZE - 1] = crate::transport::rdma::protocol::MR_AUTHORITY;
        let receipt = ReceiveWork::new(129, binding, 7, Instant::now() + Duration::from_secs(3));
        prepare(&mut queue, &receipt, &device).unwrap();
        posted(&mut queue, &receipt, runtime.verbs, 0, 1).unwrap();
        let mut completion = delivered(&mut queue, receipt.id, &receipt);
        completion.flags = 0;
        completion.invalidated_rkey = 0;
        match case {
            "invalidation" => {
                completion.flags = sys::WITH_INVALIDATE;
                completion.invalidated_rkey = receipt.rkey;
            }
            "binding" => queue.slots[0].message.memory.bytes_mut()[0] ^= 1,
            "profile" => queue.slots[0].message.memory.bytes_mut()[COMPLETION_MESSAGE_SIZE - 1] = 0,
            _ => {}
        }
        assert_eq!(
            acknowledge(&mut queue, completion).is_ok(),
            case == "valid",
            "{case}"
        );
        close(runtime, device, queue);
    }
}
