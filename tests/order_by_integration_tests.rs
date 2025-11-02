use idk_uwu_ig::cache::page_cache::{PageCache, PageCacheEntryUncompressed};
use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::ops_handler::upsert_data_into_table_column;
use idk_uwu_ig::page::Page;
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::{ColumnSpec, CreateTablePlan};
use idk_uwu_ig::ops_handler::create_table_from_plan;
use idk_uwu_ig::helpers::compressor::Compressor;
use std::sync::{Arc, RwLock};

fn build_sorted_table(
    table: &str,
    column: &str,
    order_by: Vec<String>,
    initial_values: &[&str],
) -> (Arc<PageHandler>, Arc<PageDirectory>, Arc<RwLock<TableMetaStore>>) {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(Arc::clone(&store)));

    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});

    let locator = Arc::new(PageLocator::new(Arc::clone(&directory)));
    let fetcher = Arc::new(PageFetcher::new(
        Arc::clone(&compressed_cache),
        Arc::clone(&page_io),
    ));
    let materializer = Arc::new(PageMaterializer::new(
        Arc::clone(&uncompressed_cache),
        Arc::new(Compressor::new()),
    ));
    let page_handler = Arc::new(PageHandler::new(locator, fetcher, materializer));

    let plan = CreateTablePlan::new(
        table.to_string(),
        vec![ColumnSpec::new(column, "String")],
        order_by,
        false,
    );
    create_table_from_plan(&directory, &plan).expect("create table with order by");

    let descriptor = directory
        .register_page_in_table_with_sizes(
            table,
            column,
            format!("mem://{}_{}", table, column),
            0,
            0,
            0,
            initial_values.len() as u64,
        )
        .expect("register column page");

    let mut cached = PageCacheEntryUncompressed { page: Page::new() };
    cached.page.page_metadata = descriptor.id.clone();
    for value in initial_values {
        cached.page.entries.push(Entry::new(value));
    }
    page_handler.write_back_uncompressed(&descriptor.id, cached);
    page_handler
        .update_entry_count_in_table(table, column, initial_values.len() as u64)
        .expect("sync entry count");

    (page_handler, directory, store)
}

fn collect_values(
    handler: &PageHandler,
    directory: &PageDirectory,
    table: &str,
    column: &str,
) -> Vec<String> {
    let descriptor = directory
        .latest_in_table(table, column)
        .expect("latest descriptor");
    let page = handler
        .get_page(descriptor.clone())
        .expect("load page for collection");
    page.page
        .entries
        .iter()
        .map(|entry| entry.get_data().to_string())
        .collect()
}

fn assert_metadata_consistency(
    store: &Arc<RwLock<TableMetaStore>>,
    table: &str,
    column: &str,
    expected_len: usize,
) {
    let guard = store.read().expect("metadata read lock");
    if expected_len == 0 {
        assert!(
            guard
                .locate_range(table, column, 0, 0)
                .is_empty(),
            "expected empty metadata range"
        );
        return;
    }
    let end = (expected_len - 1) as u64;
    let slices = guard.locate_range(table, column, 0, end);
    assert_eq!(
        slices.len(),
        1,
        "expected single slice for table {table}.{column}"
    );
    let slice = &slices[0];
    assert_eq!(slice.start_row_offset, 0);
    assert_eq!(slice.end_row_offset, expected_len as u64);

    let row = guard
        .locate_row(table, column, end)
        .expect("row lookup must succeed");
    assert_eq!(row.page_row_index, end);
    assert_eq!(row.descriptor.entry_count, expected_len as u64);
}

fn assert_sorted_numeric(values: &[String]) {
    let mut iter = values.iter();
    if let Some(first) = iter.next() {
        let mut prev = first.parse::<f64>().expect("numeric parse");
        for value in iter {
            let current = value.parse::<f64>().expect("numeric parse");
            assert!(
                prev <= current,
                "values not sorted numerically: {values:?}"
            );
            prev = current;
        }
    }
}

fn assert_sorted_lex(values: &[String]) {
    let mut iter = values.iter();
    if let Some(first) = iter.next() {
        let mut prev = first;
        for value in iter {
            assert!(
                prev <= value,
                "values not sorted lexicographically: {values:?}"
            );
            prev = value;
        }
    }
}

#[test]
fn order_by_numeric_random_inserts_preserve_order_and_metadata() {
    let (handler, directory, store) =
        build_sorted_table("numbers", "score", vec!["score".into()], &["10", "30"]);

    let inserts = ["25", "5", "40", "30", "-1", "17", "100"];
    for value in inserts {
        upsert_data_into_table_column(&handler, "numbers", "score", value)
            .expect("sorted insert");
        let values = collect_values(&handler, &directory, "numbers", "score");
        assert_sorted_numeric(&values);
        assert_metadata_consistency(&store, "numbers", "score", values.len());
        let descriptor = directory
            .latest_in_table("numbers", "score")
            .expect("descriptor");
        assert_eq!(descriptor.entry_count as usize, values.len());
    }
}

#[test]
fn order_by_lexicographic_inserts_keep_sorted_strings() {
    let (handler, directory, store) =
        build_sorted_table("words", "word", vec!["word".into()], &["delta", "omega"]);

    let inserts = ["alpha", "gamma", "beta", "epsilon", "eta", "alpha"];
    for value in inserts {
        upsert_data_into_table_column(&handler, "words", "word", value)
            .expect("sorted insert");
    }

    let values = collect_values(&handler, &directory, "words", "word");
    assert_sorted_lex(&values);
    assert_metadata_consistency(&store, "words", "word", values.len());
    let descriptor = directory
        .latest_in_table("words", "word")
        .expect("descriptor");
    assert_eq!(descriptor.entry_count as usize, values.len());
}

#[test]
fn order_by_numeric_duplicates_update_prefix_counts() {
    let (handler, directory, store) =
        build_sorted_table("metrics", "value", vec!["value".into()], &["0"]);

    let inserts = [
        "0", "0", "1", "-5", "3", "2", "2", "-5", "4", "3", "3", "1",
    ];

    for (idx, value) in inserts.iter().enumerate() {
        upsert_data_into_table_column(&handler, "metrics", "value", value)
            .expect("sorted insert");
        let values = collect_values(&handler, &directory, "metrics", "value");
        assert_sorted_numeric(&values);
        assert_metadata_consistency(&store, "metrics", "value", values.len());
        let descriptor = directory
            .latest_in_table("metrics", "value")
            .expect("descriptor");
        assert_eq!(
            descriptor.entry_count as usize,
            values.len(),
            "entry count mismatch after insert {idx} ({value})"
        );
    }
}
