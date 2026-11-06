use super::{abi, boundary::*, memory};
use std::mem::{align_of, offset_of, size_of, size_of_val};
use std::{ffi::c_void, ptr};

#[test]
fn rust_records_match_the_frozen_lp64_inventory() {
    macro_rules! record {
        ($c:literal, $t:ty, $($name:literal => $field:ident),+) => {{
            // SAFETY: these C records consist entirely of integers, byte arrays and raw pointers.
            let r:$t=unsafe{std::mem::zeroed()};
            let mut text=format!("{}\t{}\t{}",$c,size_of::<$t>(),align_of::<$t>());
            $(text.push_str(&format!("\t{}={}/{}",$name,offset_of!($t,$field),size_of_val(&r.$field)));)+
            text
        }}
    }
    let rows = [
        record!("tent_request_t",abi::Request,"opcode"=>opcode,"source"=>source,"target_id"=>target_id,"target_offset"=>target_offset,"length"=>length,"priority"=>priority,"transport_hint"=>transport_hint),
        record!("tent_status_t",abi::Status,"status"=>status,"transferred_bytes"=>transferred_bytes),
        record!("struct tent_buffer_info",abi::Buffer,"base"=>base,"length"=>length,"location"=>location),
        record!("tent_segment_info_t",abi::SegmentInfo,"type"=>kind,"num_buffers"=>count,"buffers"=>buffers),
        record!("tent_notifi_record_t",abi::NotifyRecord,"handle"=>handle,"name"=>name,"msg"=>msg),
        record!("tent_notifi_info",abi::NotifyInfo,"num_records"=>count,"records"=>records),
        record!("tent_memory_options_t",abi::Options,"location"=>location,"permission"=>permission,"transport_type"=>transport_type,"shm_path"=>shm_path,"shm_offset"=>shm_offset,"internal"=>internal),
        record!("tent_nic_load_stat_t",abi::NicStat,"device_name"=>device_name,"inflight_bytes"=>inflight_bytes,"ewma_bandwidth_bps"=>ewma_bandwidth_bps),
    ];
    let expected = include_str!("../reference/abi.lp64");
    assert_eq!(
        rows.as_slice(),
        expected.lines().take(rows.len()).collect::<Vec<_>>()
    );
    assert_eq!(size_of::<abi::Handle>(), 8);
}

#[test]
fn every_frozen_transport_number_has_a_disposition() {
    let mut seen = 0;
    for line in include_str!("../reference/capabilities.tsv")
        .lines()
        .skip(1)
    {
        let cols: Vec<_> = line.split('\t').collect();
        let id = cols[1].parse::<i32>().unwrap();
        match memory::transport(id) {
            Ok(t) => assert_eq!(i32::from(t.tent_id()), id),
            Err(c) => {
                assert_eq!(c, UNSUPPORTED);
                assert!(cols[11] == "cap.excluded" || ["mpcomm", "fabric"].contains(&cols[0]));
            }
        }
        seen += 1;
    }
    assert_eq!(seen, 19);
    assert_eq!(memory::transport(19), Err(INVALID));
}
#[test]
fn panic_sentinels_and_typed_statuses_are_independent_of_task_state() {
    assert_eq!(call(|| panic!("control")), -199);
    assert!(guard(ptr::null_mut::<c_void>(), || panic!("control")).is_null());
    assert_eq!(guard(0u64, || panic!("control")), 0);
    void_call("control", || panic!("control"));
    assert_eq!(Code::from(orbite::Error::ResourceExhausted), FULL);
    for path in ["metadata_type", "metadata_servers"] {
        assert_eq!(
            Code::from(orbite::Error::TentConfig {
                path: path.into(),
                reason: "external metadata stores are unsupported",
            }),
            Code(6)
        );
    }
    assert_eq!(
        Code::from(orbite::Error::Unquiesced {
            operation: "close",
            code: 5
        }),
        UNKNOWN
    );
    let f = orbite::NotificationFailure {
        error: orbite::Error::Timeout,
        effect: orbite::NotificationEffect::Unknown,
    };
    assert_eq!(Code::from(f), UNKNOWN);
}
