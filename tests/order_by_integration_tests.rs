use idk_uwu_ig::cache::page_cache::{PageCache, PageCacheEntryUncompressed};
use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::ops_handler::{
    create_table_from_plan, insert_sorted_row, upsert_data_into_table_column,
};
use idk_uwu_ig::page::Page;
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::{ColumnSpec, CreateTablePlan};
use std::cmp::Ordering;
use std::sync::{Arc, RwLock};

fn build_table_with_rows(
    table: &str,
    columns_with_data: &[(&str, Vec<&str>)],
    order_by: Vec<String>,
) -> (Arc<PageHandler>, Arc<PageDirectory>, Arc<RwLock<TableMetaStore>>) {
    assert!(
        !columns_with_data.is_empty(),
        "test helper requires at least one column"
    );

    let expected_len = columns_with_data[0].1.len();
    for (name, values) in columns_with_data {
        assert_eq!(
            values.len(),
            expected_len,
            "column {name} length mismatch"
        );
    }

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
    let handler = Arc::new(PageHandler::new(locator, fetcher, materializer));

    let column_specs: Vec<ColumnSpec> = columns_with_data
        .iter()
        .map(|(name, _)| ColumnSpec::new(*name, "String"))
        .collect();

    let plan = CreateTablePlan::new(table.to_string(), column_specs, order_by, false);
    create_table_from_plan(&directory, &plan).expect("create table with order by");

    for (name, values) in columns_with_data {
        let descriptor = directory
            .register_page_in_table_with_sizes(
                table,
                name,
                format!("mem://{table}_{name}"),
                0,
                0,
                0,
                values.len() as u64,
            )
            .expect("register column page");

        let mut cached = PageCacheEntryUncompressed { page: Page::new() };
        cached.page.page_metadata = descriptor.id.clone();
        for value in values {
            cached.page.entries.push(Entry::new(value));
        }
        handler.write_back_uncompressed(&descriptor.id, cached);
        handler
            .update_entry_count_in_table(table, name, values.len() as u64)
            .expect("sync entry count");
    }

    (handler, directory, store)
}

fn collect_column_values(
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

fn collect_table_rows(
    handler: &PageHandler,
    directory: &PageDirectory,
    table: &str,
    column_names: &[&str],
) -> Vec<Vec<String>> {
    if column_names.is_empty() {
        return Vec::new();
    }

    let row_count = directory
        .latest_in_table(table, column_names[0])
        .map(|descriptor| descriptor.entry_count as usize)
        .unwrap_or(0);

    let mut rows = Vec::with_capacity(row_count);
    for row_idx in 0..row_count {
        let mut row = Vec::with_capacity(column_names.len());
        for column in column_names {
            let entry = handler
                .read_entry_at(table, column, row_idx as u64)
                .expect("entry must exist");
            row.push(entry.get_data().to_string());
        }
        rows.push(row);
    }
    rows
}

fn assert_metadata_for_columns(
    store: &Arc<RwLock<TableMetaStore>>,
    table: &str,
    columns: &[&str],
    expected_len: usize,
) {
    let guard = store.read().expect("metadata read lock");
    for column in columns {
        if expected_len == 0 {
            assert!(
                guard.locate_range(table, column, 0, 0).is_empty(),
                "expected empty metadata for {table}.{column}"
            );
            continue;
        }

        let end = (expected_len - 1) as u64;
        let slices = guard.locate_range(table, column, 0, end);
        assert_eq!(
            slices.len(),
            1,
            "expected single slice for {table}.{column}"
        );
        let slice = &slices[0];
        assert_eq!(slice.start_row_offset, 0);
        assert_eq!(slice.end_row_offset, expected_len as u64);

        let location = guard
            .locate_row(table, column, end)
            .expect("row location must exist");
        assert_eq!(location.page_row_index, end);
        assert_eq!(location.descriptor.entry_count, expected_len as u64);
    }
}

fn compare_strs(left: &str, right: &str) -> Ordering {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(l), Ok(r)) => l.partial_cmp(&r).unwrap_or(Ordering::Equal),
        _ => left.cmp(right),
    }
}

fn assert_rows_sorted(rows: &[Vec<String>], sort_indices: &[usize]) {
    if rows.len() <= 1 {
        return;
    }

    for window in rows.windows(2) {
        let left = &window[0];
        let right = &window[1];
        let mut decided = false;
        for &idx in sort_indices {
            let cmp = compare_strs(&left[idx], &right[idx]);
            if cmp == Ordering::Greater {
                panic!(
                    "rows not sorted: left={:?}, right={:?}, sort_indices={sort_indices:?}",
                    left, right
                );
            } else if cmp == Ordering::Less {
                decided = true;
                break;
            }
        }
        if !decided {
            // All sort keys equal; order is stable so nothing to assert.
        }
    }
}

#[test]
fn order_by_numeric_random_inserts_preserve_order_and_metadata() {
    let columns = vec![("score", vec!["10", "30"])];
    let (handler, directory, store) =
        build_table_with_rows("numbers", &columns, vec!["score".into()]);

    let inserts = ["25", "5", "40", "30", "-1", "17", "100"];
    for value in inserts {
        upsert_data_into_table_column(&handler, "numbers", "score", value)
            .expect("sorted insert");
        let values = collect_column_values(&handler, &directory, "numbers", "score");
        assert_rows_sorted(&values.iter().map(|v| vec![v.clone()]).collect::<Vec<_>>(), &[0]);
        assert_metadata_for_columns(&store, "numbers", &["score"], values.len());
        let descriptor = directory
            .latest_in_table("numbers", "score")
            .expect("descriptor");
        assert_eq!(descriptor.entry_count as usize, values.len());
    }
}

#[test]
fn order_by_lexicographic_inserts_keep_sorted_strings() {
    let columns = vec![("word", vec!["delta", "omega"])];
    let (handler, directory, store) =
        build_table_with_rows("words", &columns, vec!["word".into()]);

    let inserts = ["alpha", "gamma", "beta", "epsilon", "eta", "alpha"];
    for value in inserts {
        upsert_data_into_table_column(&handler, "words", "word", value)
            .expect("sorted insert");
    }

    let values = collect_column_values(&handler, &directory, "words", "word");
    assert_rows_sorted(&values.iter().map(|v| vec![v.clone()]).collect::<Vec<_>>(), &[0]);
    assert_metadata_for_columns(&store, "words", &["word"], values.len());
    let descriptor = directory
        .latest_in_table("words", "word")
        .expect("descriptor");
    assert_eq!(descriptor.entry_count as usize, values.len());
}

#[test]
fn order_by_numeric_duplicates_update_prefix_counts() {
    let columns = vec![("value", vec!["0"])];
    let (handler, directory, store) =
        build_table_with_rows("metrics", &columns, vec!["value".into()]);

    let inserts = [
        "0", "0", "1", "-5", "3", "2", "2", "-5", "4", "3", "3", "1",
    ];

    for (idx, value) in inserts.iter().enumerate() {
        upsert_data_into_table_column(&handler, "metrics", "value", value)
            .expect("sorted insert");
        let values = collect_column_values(&handler, &directory, "metrics", "value");
        assert_rows_sorted(&values.iter().map(|v| vec![v.clone()]).collect::<Vec<_>>(), &[0]);
        assert_metadata_for_columns(&store, "metrics", &["value"], values.len());
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

#[test]
fn composite_order_by_inserts_are_sorted() {
    let columns = vec![
        ("country", vec!["CA", "US", "US"]),
        ("city", vec!["Toronto", "Austin", "Seattle"]),
        ("population", vec!["2.7", "1.0", "0.75"]),
    ];

    let (handler, directory, store) =
        build_table_with_rows("cities", &columns, vec!["country".into(), "city".into()]);

    let inserts = vec![
        vec![("country", "US"), ("city", "Boston"), ("population", "0.65")],
        vec![("country", "CA"), ("city", "Calgary"), ("population", "1.2")],
        vec![("country", "US"), ("city", "Atlanta"), ("population", "0.5")],
        vec![("country", "CA"), ("city", "Halifax"), ("population", "0.4")],
    ];

    for row in inserts {
        insert_sorted_row(&handler, "cities", &row).expect("sorted row insert");
    }

    let column_names = ["country", "city", "population"];
    let rows = collect_table_rows(&handler, &directory, "cities", &column_names);
    assert_rows_sorted(&rows, &[0, 1]);
    assert_metadata_for_columns(&store, "cities", &column_names, rows.len());
}

#[test]
fn composite_order_by_numeric_then_lexicographic() {
    let columns = vec![
        ("score", vec!["1", "2", "2"]),
        ("label", vec!["alpha", "beta", "delta"]),
        ("note", vec!["A", "B", "D"]),
    ];

    let (handler, directory, store) =
        build_table_with_rows("rankings", &columns, vec!["score".into(), "label".into()]);

    let inserts = vec![
        vec![("score", "1"), ("label", "aardvark"), ("note", "AA")],
        vec![("score", "3"), ("label", "gamma"), ("note", "G")],
        vec![("score", "2"), ("label", "charlie"), ("note", "C")],
        vec![("score", "2"), ("label", "beta"), ("note", "B2")],
        vec![("score", "0"), ("label", "omega"), ("note", "Z")],
    ];

    for row in inserts {
        insert_sorted_row(&handler, "rankings", &row).expect("sorted row insert");
    }

    let column_names = ["score", "label", "note"];
    let rows = collect_table_rows(&handler, &directory, "rankings", &column_names);
    assert_rows_sorted(&rows, &[0, 1]);
    assert_metadata_for_columns(&store, "rankings", &column_names, rows.len());
}
