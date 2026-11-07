use super::*;

fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks_exact(2)
        .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn frozen_rpc_fixtures_match_request_and_response_serialization() {
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("../../../reference/wire/rpc.json")).unwrap();
    for (_name, case) in corpus.as_object().unwrap() {
        let req = &case["request"];
        let header: [u8; REQUEST] = hex(req["header_hex"].as_str().unwrap()).try_into().unwrap();
        let body: [u8; ARGUMENT] = hex(req["body_hex"].as_str().unwrap()).try_into().unwrap();
        let (seq, size) = request_header(&header, 65536).unwrap();
        assert_eq!(seq, 0);
        let bytes = request(argument(&body).unwrap(), size).unwrap();
        assert_eq!(&bytes[..REQUEST], &header);
        assert_eq!(&bytes[REQUEST..], &body);
        let resp = &case["response"];
        let header: [u8; RESPONSE] = hex(resp["header_hex"].as_str().unwrap())
            .try_into()
            .unwrap();
        if header[2] == 0 {
            let size = response_header(&header, 65536).unwrap();
            assert_eq!(response(0, size).unwrap(), header);
            assert_eq!(hex(resp["body_hex"].as_str().unwrap()), RETURN);
        } else {
            assert!(response_header(&header, 65536).is_err());
        }
    }
}

#[test]
fn magic_version_serialization_sequence_and_lengths_fail_closed() {
    let req = request(1, 0).unwrap();
    for at in [0, 1, 2, 3, 8, 12] {
        let mut h: [u8; REQUEST] = req[..REQUEST].try_into().unwrap();
        h[at] ^= 1;
        assert!(matches!(request_header(&h, 1024), Err(Error::Protocol(_))));
    }
    for at in [0, 1, 3, 4, 8] {
        let mut h = response(0, 0).unwrap();
        h[at] ^= 1;
        assert!(matches!(response_header(&h, 1024), Err(Error::Protocol(_))));
    }
    let h = request(1, 1025).unwrap();
    assert!(matches!(
        request_header(h[..REQUEST].try_into().unwrap(), 1024),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(
        response_header(&response(0, 1025).unwrap(), 1024),
        Err(Error::ResourceExhausted)
    ));
    let mut body = RETURN;
    body[0] ^= 1;
    assert!(return_body(&body).is_err());
}
