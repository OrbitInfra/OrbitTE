use super::link_speed_mbps;

#[test]
fn negotiated_speed_and_width_use_tent_units_and_unknown_is_not_guessed() {
    for (speed, width, mbps) in [
        (1, 1, 2500),
        (4, 2, 40000),
        (16, 4, 112000),
        (32, 8, 300000),
        (64, 2, 200000),
        (128, 2, 400000),
        (128, 16, 200000),
    ] {
        assert_eq!(link_speed_mbps(speed, width), Some(mbps));
    }
    for (speed, width) in [(0, 2), (3, 2), (128, 0), (128, 3)] {
        assert_eq!(link_speed_mbps(speed, width), None);
    }
}
