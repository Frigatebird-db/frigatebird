use frigatebird::cache::page_cache::PageCacheEntryUncompressed;
use frigatebird::entry::Entry;
use frigatebird::helpers::compressor::Compressor;
use frigatebird::page::Page;
use std::sync::Arc;

fn create_page_with_entries(count: usize, data_size: usize) -> Page {
    let mut page = Page::new();
    let data = "x".repeat(data_size);
    for i in 0..count {
        page.add_entry(Entry::new(&format!("{}{}", data, i)));
    }
    page
}

#[test]
fn compressor_new_creates_instance() {
    let compressor = Compressor::new();
    let _ = format!("{:?}", &compressor as *const _);
}

#[test]
fn compress_then_decompress_empty_page() {
    let compressor = Compressor::new();
    let page = Page::new();
    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));

    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(uncompressed.page.len(), decompressed.entries.len());
}

#[test]
fn compress_then_decompress_single_entry() {
    let compressor = Compressor::new();
    let mut page = Page::new();
    page.add_entry(Entry::new("test_data"));

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(1, decompressed.entries.len());
}

#[test]
fn compress_then_decompress_multiple_entries() {
    let compressor = Compressor::new();
    let page = create_page_with_entries(50, 100);
    let original_count = page.entries.len();

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(original_count, decompressed.entries.len());
}

#[test]
fn compress_actually_compresses() {
    let compressor = Compressor::new();

    // Highly compressible data
    let page = create_page_with_entries(100, 1000);
    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));

    let uncompressed_size = bincode::serialize(&uncompressed.page.as_disk_page())
        .unwrap()
        .len();
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let compressed_size = compressed.page.len();

    // LZ4 should compress repetitive data significantly
    assert!(compressed_size < uncompressed_size);
}

#[test]
fn compress_large_page() {
    let compressor = Compressor::new();
    let page = create_page_with_entries(1000, 500);

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(uncompressed.page.len(), decompressed.entries.len());
}

#[test]
fn compress_incompressible_data() {
    let compressor = Compressor::new();
    let mut page = Page::new();

    // Pseudo-random incompressible data
    for i in 0..100 {
        let data: String = (0..100)
            .map(|j| ((i * 256 + j * 7) % 256) as u8 as char)
            .collect();
        page.add_entry(Entry::new(&data));
    }

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(uncompressed.page.len(), decompressed.entries.len());
}

#[test]
fn compress_with_special_characters() {
    let compressor = Compressor::new();
    let mut page = Page::new();
    page.add_entry(Entry::new("test\n\t\r\"'\\😀"));
    page.add_entry(Entry::new("🚀🌟💻"));
    page.add_entry(Entry::new("mixed ascii 123 and unicode ∑∫√"));

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(3, decompressed.entries.len());
}

#[test]
fn compress_empty_entries() {
    let compressor = Compressor::new();
    let mut page = Page::new();
    page.add_entry(Entry::new(""));
    page.add_entry(Entry::new(""));
    page.add_entry(Entry::new(""));

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(3, decompressed.entries.len());
}

#[test]
fn compress_decompress_multiple_times() {
    let compressor = Compressor::new();
    let page = create_page_with_entries(20, 50);

    let mut current = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));

    for _ in 0..5 {
        let compressed = compressor.compress(Arc::clone(&current));
        let disk_page = compressor.decompress(Arc::new(compressed));
        current = Arc::new(PageCacheEntryUncompressed::from_disk_page(
            disk_page,
            frigatebird::sql::DataType::String,
        ));
    }

    assert_eq!(20, current.page.num_rows);
}

#[test]
fn compress_very_large_entry() {
    let compressor = Compressor::new();
    let mut page = Page::new();

    // 1MB entry
    let large_data = "x".repeat(1_000_000);
    page.add_entry(Entry::new(&large_data));

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let compressed_size = compressed.page.len();
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(1, decompressed.entries.len());

    // Highly compressible data should compress significantly
    let original_size = bincode::serialize(&uncompressed.page.as_disk_page())
        .unwrap()
        .len();
    assert!(compressed_size < original_size / 10);
}

#[test]
fn compress_arc_sharing() {
    let compressor = Compressor::new();
    let page = create_page_with_entries(10, 100);
    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        frigatebird::sql::DataType::String,
    ));

    let arc1 = Arc::clone(&uncompressed);
    let arc2 = Arc::clone(&uncompressed);

    let compressed1 = compressor.compress(arc1);
    let compressed2 = compressor.compress(arc2);

    // Both should produce identical compressed data
    assert_eq!(compressed1.page.len(), compressed2.page.len());
}
