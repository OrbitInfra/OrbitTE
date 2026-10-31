use std::error::Error;
use std::time::Duration;

use orbit_te::{
    EngineConfig, Notification, NotificationStatus, Permission, TransferEngine, TransferOp,
    TransferRequest,
};

fn main() -> Result<(), Box<dyn Error>> {
    let source = TransferEngine::new(EngineConfig::default())?;
    let exported =
        source.register_memory(vec![0xa7; 32].into_boxed_slice(), Permission::RemoteRead)?;
    let requester = TransferEngine::new(EngineConfig::default())?;
    let local = requester.register_memory(vec![0; 32].into_boxed_slice(), Permission::LocalOnly)?;
    let peer = requester.open_segment(
        source.endpoint(),
        source.source_epoch(),
        Duration::from_secs(2),
    )?;
    let notification = Notification {
        name: "example-scope".into(),
        message: "read-complete".into(),
    };
    let report = requester.submit_and_notify(
        TransferOp::Read,
        &peer,
        &[TransferRequest {
            remote_staging: false,
            transport: orbit_te::Transport::Tcp,
            local_address: local.base_address(),
            remote_address: peer.regions()[0].base_address(),
            length: 32,
        }],
        &notification,
        Duration::from_secs(2),
    )?;
    if let Some(failure) = report.failure {
        return Err(failure.error.into());
    }
    match report.notification {
        NotificationStatus::Delivered => {}
        NotificationStatus::Failed(failure) => return Err(failure.into()),
        other => return Err(format!("notification was not delivered: {other:?}").into()),
    }
    let mut bytes = [0; 32];
    local.read(0, &mut bytes)?;
    assert_eq!(bytes, [0xa7; 32]);
    assert_eq!(source.receive_notifications()?, [notification]);
    assert!(source.receive_notifications()?.is_empty());
    source.close_source(Duration::from_secs(2))?;
    source.unregister_memory(&exported)?;
    println!(
        "{} bytes read; completion notification received once",
        report.transferred_bytes
    );
    Ok(())
}
