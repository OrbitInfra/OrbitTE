use super::*;

#[test]
fn sparse_nodes_intersect_the_inherited_cpuset_before_round_robin() {
    let root = std::env::temp_dir().join(format!("orbitte-affinity-{}", std::process::id()));
    let nodes = root.join("devices/system/node");
    for (node, cpus) in [(0, "0-3"), (2, "4-7"), (7, "8,10-12")] {
        let dir = nodes.join(format!("node{node}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("cpulist"), cpus).unwrap();
    }
    let groups = intersect(&root, vec![5, 8, 12]).unwrap();
    assert_eq!(
        groups,
        vec![
            CpuAffinity {
                node: Some(2),
                cpus: vec![5]
            },
            CpuAffinity {
                node: Some(7),
                cpus: vec![8, 12]
            }
        ]
    );
    assert_eq!(cpulist("0,2-4,4"), Some(vec![0, 2, 3, 4]));
    for bad in ["4-2", "1-2-3", "x", "1048576"] {
        assert!(cpulist(bad).is_none());
    }
    assert_eq!(
        intersect(&root.join("missing"), vec![5]).unwrap(),
        vec![CpuAffinity {
            node: None,
            cpus: vec![5]
        }]
    );
    assert!(intersect(&root, vec![]).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn actual_worker_binding_is_verified_without_changing_the_creator() {
    let creator = allowed_cpus().unwrap();
    let groups = worker_affinities().unwrap();
    for group in groups {
        assert!(
            group
                .cpus
                .iter()
                .all(|cpu| creator.binary_search(cpu).is_ok())
        );
        std::thread::spawn(move || {
            group.bind().unwrap();
            assert_eq!(allowed_cpus().unwrap(), group.cpus);
        })
        .join()
        .unwrap();
    }
    assert_eq!(allowed_cpus().unwrap(), creator);
}
