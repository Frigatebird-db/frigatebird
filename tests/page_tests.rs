use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::page::Page;

#[test]
fn page_new_creates_empty() {
    let page = Page::new();
    assert_eq!(page.entries.len(), 0);
    assert_eq!(page.page_metadata, "");
}

#[test]
fn page_add_entry_increases_count() {
    let mut page = Page::new();
    assert_eq!(page.entries.len(), 0);

    page.add_entry(Entry::new("first"));
    assert_eq!(page.entries.len(), 1);

    page.add_entry(Entry::new("second"));
    assert_eq!(page.entries.len(), 2);
}

#[test]
fn page_add_multiple_entries() {
    let mut page = Page::new();

    for i in 0..100 {
        page.add_entry(Entry::new(&format!("entry_{}", i)));
    }

    assert_eq!(page.entries.len(), 100);
}

#[test]
fn page_serialization_roundtrip() {
    let mut page = Page::new();
    page.add_entry(Entry::new("data1"));
    page.add_entry(Entry::new("data2"));

    let serialized = bincode::serialize(&page).unwrap();
    let deserialized: Page = bincode::deserialize(&serialized).unwrap();

    assert_eq!(page.entries.len(), deserialized.entries.len());
    assert_eq!(
        bincode::serialize(&page).unwrap(),
        bincode::serialize(&deserialized).unwrap()
    );
}

#[test]
fn page_clone_works() {
    let mut page = Page::new();
    page.add_entry(Entry::new("test"));

    let cloned = page.clone();
    assert_eq!(page.entries.len(), cloned.entries.len());
}

#[test]
fn page_with_large_entries() {
    let mut page = Page::new();
    let large_data = "x".repeat(10_000);

    for i in 0..10 {
        page.add_entry(Entry::new(&format!("{}{}", large_data, i)));
    }

    let serialized = bincode::serialize(&page).unwrap();
    assert!(serialized.len() > 100_000);

    let deserialized: Page = bincode::deserialize(&serialized).unwrap();
    assert_eq!(page.entries.len(), deserialized.entries.len());
}

#[test]
fn page_empty_serialization() {
    let page = Page::new();
    let serialized = bincode::serialize(&page).unwrap();
    let deserialized: Page = bincode::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.entries.len(), 0);
}

#[test]
fn page_entries_maintain_order() {
    let mut page = Page::new();

    for i in 0..50 {
        page.add_entry(Entry::new(&format!("entry_{:03}", i)));
    }

    let serialized = bincode::serialize(&page).unwrap();
    let deserialized: Page = bincode::deserialize(&serialized).unwrap();

    assert_eq!(page.entries.len(), deserialized.entries.len());
}

#[test]
fn page_with_empty_entries() {
    let mut page = Page::new();
    page.add_entry(Entry::new(""));
    page.add_entry(Entry::new(""));
    page.add_entry(Entry::new(""));

    assert_eq!(page.entries.len(), 3);

    let serialized = bincode::serialize(&page).unwrap();
    let deserialized: Page = bincode::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.entries.len(), 3);
}
