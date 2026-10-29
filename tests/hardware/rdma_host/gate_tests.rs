//! CPU controls for the control-plane boundary; no verbs or GPU are opened.

use super::*;

fn wire<const N: usize>(code: u8, length: u64) -> [u8; N] {
    let mut bytes = [0; N];
    bytes[..8].copy_from_slice(b"ORBITTE\0");
    bytes[8..10].copy_from_slice(&20u16.to_be_bytes());
    bytes[10] = code;
    bytes[28..36].copy_from_slice(&length.to_be_bytes());
    bytes
}

fn phase(code: u8) -> [u8; 142] {
    let mut bytes = [0x71; 142];
    bytes[0] = code;
    bytes
}

#[test]
fn grant_is_forwarded_or_aborted_only_after_source_ready() -> Result<()> {
    for armed in [false, true] {
        let upstream = TcpListener::bind("127.0.0.1:0")?;
        let gate = GrantGate::new("127.0.0.1:0".parse()?, upstream.local_addr()?)?;
        if armed {
            gate.arm()?;
        }
        let native = thread::spawn(move || -> Result<()> {
            let (mut stream, _) = upstream.accept()?;
            stream.set_read_timeout(Some(TIMEOUT))?;
            assert_eq!(frame::<52>(&mut stream)?, wire::<52>(4, 1));
            stream.write_all(&wire::<36>(0, 188))?;
            stream.write_all(&[0x31; 188])?;
            assert_eq!(frame::<106>(&mut stream)?, [0x41; 106]);
            stream.write_all(&wire::<36>(0, 89))?;
            stream.write_all(&[0x51; 89])?;
            assert_eq!(frame::<142>(&mut stream)?, phase(1));
            assert_eq!(frame::<40>(&mut stream)?, [0x61; 40]);
            stream.write_all(&wire::<36>(9, 0))?;
            stream.write_all(&phase(2))?;
            stream.write_all(&[0x81; 40])?;
            if armed {
                assert!(stream.read_exact(&mut [0]).is_err());
            } else {
                assert_eq!(frame::<142>(&mut stream)?, phase(3));
                stream.write_all(&wire::<36>(0, 0))?;
                stream.write_all(&phase(4))?;
                stream.write_all(&phase(5))?;
            }
            Ok(())
        });
        let mut client = TcpStream::connect(gate.address())?;
        client.set_read_timeout(Some(TIMEOUT))?;
        client.write_all(&wire::<52>(4, 1))?;
        assert_eq!(frame::<36>(&mut client)?, wire::<36>(0, 188));
        assert_eq!(frame::<188>(&mut client)?, [0x31; 188]);
        client.write_all(&[0x41; 106])?;
        assert_eq!(frame::<36>(&mut client)?, wire::<36>(0, 89));
        assert_eq!(frame::<89>(&mut client)?, [0x51; 89]);
        client.write_all(&phase(1))?;
        client.write_all(&[0x61; 40])?;
        if armed {
            let deadline = Instant::now() + TIMEOUT;
            while gate.held()? != 1 {
                assert!(Instant::now() < deadline);
                thread::sleep(Duration::from_millis(1));
            }
            client.set_read_timeout(Some(Duration::from_millis(5)))?;
            let error = client
                .read_exact(&mut [0])
                .expect_err("GRANT escaped armed gate");
            assert!(matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ));
            gate.abort()?;
            client.set_read_timeout(Some(TIMEOUT))?;
            assert!(client.read_exact(&mut [0]).is_err());
            assert_eq!(gate.held()?, 0);
            gate.arm()?;
            gate.abort()?;
        } else {
            assert_eq!(frame::<36>(&mut client)?, wire::<36>(9, 0));
            assert_eq!(frame::<142>(&mut client)?, phase(2));
            assert_eq!(frame::<40>(&mut client)?, [0x81; 40]);
            client.write_all(&phase(3))?;
            assert_eq!(frame::<36>(&mut client)?, wire::<36>(0, 0));
            assert_eq!(frame::<142>(&mut client)?, phase(4));
            assert_eq!(frame::<142>(&mut client)?, phase(5));
        }
        native.join().map_err(|_| "fake source panicked")??;
        gate.close()?;
    }
    Ok(())
}

#[test]
fn incompatible_frames_cannot_fall_back_to_tcp_payload() -> Result<()> {
    for operation in [1, 2, 3, 6, 7, 255] {
        let upstream = TcpListener::bind("127.0.0.1:0")?;
        upstream.set_nonblocking(true)?;
        let gate = GrantGate::new("127.0.0.1:0".parse()?, upstream.local_addr()?)?;
        let mut client = TcpStream::connect(gate.address())?;
        client.set_read_timeout(Some(TIMEOUT))?;
        client.write_all(&wire::<52>(operation, 1))?;
        assert!(client.read_exact(&mut [0]).is_err());
        assert!(upstream.accept().is_err());
        assert!(gate.close().is_err());
    }
    let mut old = wire::<36>(0, 0);
    old[9] = 13;
    assert!(header(&old).is_err());
    Ok(())
}
