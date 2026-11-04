//! Classification is evidence for a window epoch. Rail recovery remains with RailMonitor.
use ibverbs_sys as ffi;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Congestion,
    ReceiverPressure,
    RouteTimeout,
    LocalConfiguration,
    RemoteMetadata,
    DerivedFlush,
    Fatal,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureScope {
    Operation,
    Qp,
    Cq,
    Route,
    Port,
    Device,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Classification {
    pub outcome: Outcome,
    pub scope: FailureScope,
    pub root_failure: bool,
    pub vendor_error: u32,
}

pub fn classify_completion(status: u32, vendor_error: u32) -> Classification {
    use FailureScope::*;
    use Outcome::*;
    use ffi::ibv_wc_status::*;
    let (outcome, scope) = match status {
        IBV_WC_SUCCESS => (Success, Operation),
        IBV_WC_RNR_RETRY_EXC_ERR => (ReceiverPressure, Qp),
        IBV_WC_RETRY_EXC_ERR | IBV_WC_RESP_TIMEOUT_ERR => (RouteTimeout, Qp),
        IBV_WC_LOC_LEN_ERR | IBV_WC_LOC_PROT_ERR | IBV_WC_MW_BIND_ERR | IBV_WC_LOC_ACCESS_ERR => {
            (LocalConfiguration, Operation)
        }
        IBV_WC_LOC_QP_OP_ERR
        | IBV_WC_LOC_EEC_OP_ERR
        | IBV_WC_LOC_RDD_VIOL_ERR
        | IBV_WC_INV_EECN_ERR
        | IBV_WC_INV_EEC_STATE_ERR => (LocalConfiguration, Qp),
        IBV_WC_BAD_RESP_ERR
        | IBV_WC_REM_INV_REQ_ERR
        | IBV_WC_REM_ACCESS_ERR
        | IBV_WC_REM_OP_ERR
        | IBV_WC_REM_INV_RD_REQ_ERR
        | IBV_WC_REM_ABORT_ERR => (RemoteMetadata, Operation),
        IBV_WC_WR_FLUSH_ERR => (DerivedFlush, Qp),
        _ => (Fatal, Qp),
    };
    Classification {
        outcome,
        scope,
        root_failure: !matches!(outcome, Success | DerivedFlush),
        vendor_error,
    }
}
pub fn classify_async_event(event: u32) -> Option<Classification> {
    let scope = match event {
        ffi::IBV_EVENT_QP_FATAL
        | ffi::IBV_EVENT_QP_REQ_ERR
        | ffi::IBV_EVENT_QP_ACCESS_ERR
        | ffi::IBV_EVENT_WQ_FATAL
        | ffi::IBV_EVENT_SRQ_ERR => FailureScope::Qp,
        ffi::IBV_EVENT_CQ_ERR => FailureScope::Cq,
        ffi::IBV_EVENT_PATH_MIG_ERR => FailureScope::Route,
        ffi::IBV_EVENT_PORT_ERR => FailureScope::Port,
        ffi::IBV_EVENT_DEVICE_FATAL => FailureScope::Device,
        _ => return None,
    };
    Some(Classification {
        outcome: Outcome::Fatal,
        scope,
        root_failure: true,
        vendor_error: 0,
    })
}
