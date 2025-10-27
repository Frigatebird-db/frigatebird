use idk_uwu_ig::entry::{Entry, current_epoch_millis};
use std::thread;
use std::time::Duration;

#[test]
fn entry_new_creates_with_data() {
    let entry = Entry::new("test_data");
    let serialized = bincode::serialize(&entry).unwrap();
    let deserialized: Entry = bincode::deserialize(&serialized).unwrap();
    assert_eq!(
        bincode::serialize(&entry).unwrap(),
        bincode::serialize(&deserialized).unwrap()
    );
}

#[test]
fn entry_new_with_empty_string() {
    let entry = Entry::new("");
    let serialized = bincode::serialize(&entry).unwrap();
    assert!(!serialized.is_empty());
}

#[test]
fn entry_new_with_large_data() {
    let large_data = "x".repeat(1_000_000);
    let entry = Entry::new(&large_data);
    let serialized = bincode::serialize(&entry).unwrap();
    let deserialized: Entry = bincode::deserialize(&serialized).unwrap();
    assert_eq!(
        bincode::serialize(&entry).unwrap(),
        bincode::serialize(&deserialized).unwrap()
    );
}

#[test]
fn entry_clone_works() {
    let entry = Entry::new("clone_test");
    let cloned = entry.clone();
    assert_eq!(
        bincode::serialize(&entry).unwrap(),
        bincode::serialize(&cloned).unwrap()
    );
}

#[test]
fn entry_with_special_characters() {
    let special = "test\n\t\r\"'\\😀🚀";
    let entry = Entry::new(special);
    let serialized = bincode::serialize(&entry).unwrap();
    let deserialized: Entry = bincode::deserialize(&serialized).unwrap();
    assert_eq!(
        bincode::serialize(&entry).unwrap(),
        bincode::serialize(&deserialized).unwrap()
    );
}

#[test]
fn current_epoch_millis_returns_positive() {
    let time = current_epoch_millis();
    assert!(time > 0);
}

#[test]
fn current_epoch_millis_increases() {
    let time1 = current_epoch_millis();
    thread::sleep(Duration::from_millis(10));
    let time2 = current_epoch_millis();
    assert!(time2 > time1);
}

#[test]
fn current_epoch_millis_reasonable_range() {
    let time = current_epoch_millis();
    // Should be after 2020 and before 2100
    assert!(time > 1_577_836_800_000); // 2020-01-01
    assert!(time < 4_102_444_800_000); // 2100-01-01
}

#[test]
fn entry_serialization_roundtrip() {
    let entries = vec![
        Entry::new(""),
        Entry::new("simple"),
        Entry::new("with spaces and punctuation!"),
        Entry::new(&"long".repeat(1000)),
    ];

    for entry in entries {
        let serialized = bincode::serialize(&entry).unwrap();
        let deserialized: Entry = bincode::deserialize(&serialized).unwrap();
        assert_eq!(
            bincode::serialize(&entry).unwrap(),
            bincode::serialize(&deserialized).unwrap()
        );
    }
}
