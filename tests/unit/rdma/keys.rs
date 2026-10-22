use super::*;

#[test]
fn every_tag_is_burned_once_even_when_native_allocation_changes_its_initial_tag() {
    let mut keys = Keys::new(1);
    let mut used = std::collections::HashSet::new();
    for _ in 0..255u32 {
        let native = 0x12345601;
        let key = keys.burn(native).unwrap();
        assert_ne!(key, native);
        assert_eq!(key & !255, 0x12345600);
        assert!(used.insert(key));
    }
    assert!(matches!(
        keys.burn(0x12345601),
        Err(Error::ResourceExhausted)
    ));
    // A different initial tag may make the sole never-published value eligible.
    let final_key = keys.burn(0x12345602).unwrap();
    assert_eq!(final_key, 0x12345601);
    assert!(used.insert(final_key));
    assert_eq!(used.len(), 256);
    assert_eq!(keys.len(), 1);
    for tag in 0..256 {
        assert!(matches!(
            keys.burn(0x12345600 | tag),
            Err(Error::ResourceExhausted)
        ));
    }
    assert!(matches!(
        keys.burn(0x65432101),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(keys.len(), 1);
}

#[test]
fn index_capacity_preserves_old_history_and_existing_index_capacity() {
    let mut keys = Keys::new(2);
    assert_eq!(keys.burn(0x101).unwrap(), 0x102);
    assert_eq!(keys.burn(0x201).unwrap(), 0x202);
    assert!(matches!(keys.burn(0x301), Err(Error::ResourceExhausted)));
    assert_eq!(keys.burn(0x101).unwrap(), 0x103);
    assert_eq!(keys.burn(0x201).unwrap(), 0x203);
    assert_eq!(keys.len(), 2);
}
