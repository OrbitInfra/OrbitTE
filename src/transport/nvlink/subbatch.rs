use crate::Transport;
use crate::engine::PreparedTransfer;

/// Preserve request order. Never enlarge source authority beyond one region or
/// its normal transfer limit, and never reuse a mapping on another CUDA context.
pub(super) fn subbatch_len(requests: &[PreparedTransfer], max_bytes: usize) -> usize {
    let first = &requests[0];
    let context = |request: &PreparedTransfer| {
        request
            .local
            .ipc_range(request.local_offset, request.length)?
            .0
            .ipc_context()
    };
    let Ok(first_context) = context(first) else {
        return 1;
    };
    let mut start = first.remote_offset;
    let mut end = start + first.length;
    let mut count = 1;
    for request in &requests[1..] {
        if request.transport != Transport::Nvlink
            || request.remote.id != first.remote.id
            || context(request).ok() != Some(first_context)
        {
            break;
        }
        let next_start = start.min(request.remote_offset);
        let next_end = end.max(request.remote_offset + request.length);
        if next_end - next_start > max_bytes {
            break;
        }
        start = next_start;
        end = next_end;
        count += 1;
    }
    count
}
