use super::*;

#[test]
fn named_gpu_key_query_rejects_a_wider_or_different_authority() {
    for (permission, access) in [(1, 0x9600), (2, 0xae00)] {
        let create = create_command(
            0x123, 0x5667, 0x123456, 0x708192a3, 19, permission, 0x345678cd,
        );
        assert_eq!(word(&create, 0), 0x0200_0000);
        assert_eq!(word(&create, 8), 0x0034_5678);
        assert_eq!(word(&create, 16), access);
        assert_eq!(word(&create, 20), 0x1234_56cd);
        assert_eq!((word(&create, 68), word(&create, 96)), (4, 1));
        assert_eq!(word(&create, 28), 0x0100_0123);
        assert_eq!(
            &create[32..48],
            &[0, 0, 0, 0, 0x70, 0x81, 0x92, 0xa3, 0, 0, 0, 0, 0, 0, 0, 19]
        );
        assert_eq!(
            &create[272..288],
            &[
                0, 0, 0, 19, 0, 0, 0x56, 0x67, 0, 0, 0, 0, 0x70, 0x81, 0x92, 0xa3
            ]
        );
        assert!(create[288..].iter().all(|&v| v == 0));
        let mut query = [0; KLM + 32];
        query[16..80].copy_from_slice(&create[16..80]);
        query[KLM..KLM + 16].copy_from_slice(&create[KLM..KLM + 16]);
        assert!(confirms_grant(&create, &query));
        for (offset, bit) in [
            (0, 1),
            (16, 0x40),
            (18, 0x40),
            (18, 0x20),
            (18, 0x10),
            (18, 1),
            (20, 0xff),
            (23, 1),
            (28, 0x80),
            (28, 1),
            (31, 1),
            (39, 1),
            (47, 1),
            (KLM + 3, 1),
            (KLM + 7, 1),
            (KLM + 15, 1),
        ] {
            let mut wrong = query;
            wrong[offset] ^= bit;
            assert!(
                !confirms_grant(&create, &wrong),
                "query mismatch at {offset}"
            );
        }
    }
}
