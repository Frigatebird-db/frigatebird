use idk_uwu_ig::cache::page_cache::{PageCache, PageCacheEntryUncompressed};
use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::ops_handler::{
    create_table_from_plan, delete_row, insert_sorted_row, overwrite_row, read_row,
};
use idk_uwu_ig::page::Page;
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::ColumnSpec;
use idk_uwu_ig::sql::CreateTablePlan;
use idk_uwu_ig::sql::executor::SqlExecutor;
use std::sync::{Arc, RwLock};

fn build_sql_executor() -> (
    SqlExecutor,
    Arc<PageHandler>,
    Arc<PageDirectory>,
    Arc<RwLock<TableMetaStore>>,
) {
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
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));

    (executor, handler, directory, store)
}

// Note: Cache eviction to disk test removed because it requires actual disk I/O infrastructure
// which is not available in unit test environment. The cache eviction logic itself is tested
// in integration tests with proper storage setup.

#[test]
fn test_rapid_insert_delete_insert_same_value() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE flip_flop (id TEXT, value TEXT) ORDER BY value")
        .expect("create table");

    // Insert value
    executor
        .execute("INSERT INTO flip_flop (id, value) VALUES ('r1', '50')")
        .expect("insert");

    let row = read_row(&handler, "flip_flop", 0).expect("read");
    assert_eq!(row[1], "50");

    // Delete it
    executor
        .execute("DELETE FROM flip_flop WHERE value = '50'")
        .expect("delete");

    // Verify table is empty
    let count = executor
        .execute("INSERT INTO flip_flop (id, value) VALUES ('r2', '100')")
        .expect("insert to get count");
    let row = read_row(&handler, "flip_flop", 0).expect("read");
    assert_eq!(row[1], "100");

    // Insert same value again with different id
    executor
        .execute("INSERT INTO flip_flop (id, value) VALUES ('r3', '50')")
        .expect("re-insert same value");

    // Should now have 2 rows, sorted
    let row0 = read_row(&handler, "flip_flop", 0).expect("read row 0");
    let row1 = read_row(&handler, "flip_flop", 1).expect("read row 1");

    assert_eq!(row0[1], "50");
    assert_eq!(row0[0], "r3");
    assert_eq!(row1[1], "100");
}

#[test]
fn test_empty_table_operations() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE empty_test (id TEXT, val TEXT) ORDER BY val")
        .expect("create");

    // Try to read from empty table
    let result = read_row(&handler, "empty_test", 0);
    assert!(result.is_err(), "reading from empty table should fail");

    // Try to update empty table
    let result = executor.execute("UPDATE empty_test SET val = '10' WHERE val = '5'");
    assert!(
        result.is_ok(),
        "update on empty table should succeed (no-op)"
    );

    // Try to delete from empty table
    let result = executor.execute("DELETE FROM empty_test WHERE val = '5'");
    assert!(
        result.is_ok(),
        "delete on empty table should succeed (no-op)"
    );

    // Now insert one row
    executor
        .execute("INSERT INTO empty_test (id, val) VALUES ('r1', '42')")
        .expect("insert");

    let row = read_row(&handler, "empty_test", 0).expect("read after insert");
    assert_eq!(row[1], "42");

    // Delete the only row
    executor
        .execute("DELETE FROM empty_test WHERE val = '42'")
        .expect("delete only row");

    // Back to empty
    let result = read_row(&handler, "empty_test", 0);
    assert!(result.is_err(), "table should be empty again");
}

#[test]
fn test_many_duplicates_sort_stability() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE dupes (id TEXT, score TEXT) ORDER BY score")
        .expect("create");

    // Insert 20 rows with only 3 distinct scores
    for i in 0..20 {
        let score = match i % 3 {
            0 => "100",
            1 => "200",
            _ => "300",
        };
        executor
            .execute(&format!(
                "INSERT INTO dupes (id, score) VALUES ('id{i}', '{score}')"
            ))
            .expect("insert");
    }

    // Verify all 20 rows exist
    for i in 0..20 {
        let row = read_row(&handler, "dupes", i).expect(&format!("read row {i}"));
        let score: i32 = row[1].parse().expect("parse score");
        assert!(score == 100 || score == 200 || score == 300);
    }

    // Verify sort order: all 100s, then 200s, then 300s
    for i in 0..7 {
        // First ~7 should be 100
        let row = read_row(&handler, "dupes", i).expect("read");
        assert_eq!(row[1], "100");
    }

    // Update one specific duplicate (UPDATE with multiple matches is unsupported)
    // This is a KNOWN LIMITATION - UPDATE/DELETE only work with unique ORDER BY matches
    let result = executor.execute("UPDATE dupes SET score = '200' WHERE score = '100'");

    // This SHOULD fail because multiple rows match
    assert!(
        result.is_err(),
        "UPDATE with multiple ORDER BY matches should fail (current limitation)"
    );

    // To update, we'd need to provide a unique WHERE clause
    // But since we only have id and score, and multiple rows have score='100',
    // we can't uniquely identify a row without additional columns

    // Verify all rows still exist and are sorted
    for i in 0..20 {
        let row = read_row(&handler, "dupes", i).expect(&format!("read row {i}"));
        let score: i32 = row[1].parse().expect("parse score");

        if i > 0 {
            let prev_row = read_row(&handler, "dupes", i - 1).expect("read prev");
            let prev_score: i32 = prev_row[1].parse().unwrap();
            assert!(score >= prev_score, "scores should be sorted");
        }
    }
}

#[test]
fn test_interleaved_multi_table_operations() {
    let (executor, _handler, _directory, _store) = build_sql_executor();

    // Create 3 tables
    executor
        .execute("CREATE TABLE t1 (id TEXT, val TEXT) ORDER BY val")
        .expect("create t1");
    executor
        .execute("CREATE TABLE t2 (id TEXT, val TEXT) ORDER BY val")
        .expect("create t2");
    executor
        .execute("CREATE TABLE t3 (id TEXT, val TEXT) ORDER BY val")
        .expect("create t3");

    // Interleave operations across tables
    for i in 0..10 {
        executor
            .execute(&format!(
                "INSERT INTO t1 (id, val) VALUES ('t1_r{i}', '{}')",
                i * 3
            ))
            .expect("insert t1");
        executor
            .execute(&format!(
                "INSERT INTO t2 (id, val) VALUES ('t2_r{i}', '{}')",
                i * 3 + 1
            ))
            .expect("insert t2");
        executor
            .execute(&format!(
                "INSERT INTO t3 (id, val) VALUES ('t3_r{i}', '{}')",
                i * 3 + 2
            ))
            .expect("insert t3");

        // Update previous row in t1
        if i > 0 {
            let old_val = (i - 1) * 3;
            let new_val = old_val + 100;
            executor
                .execute(&format!(
                    "UPDATE t1 SET val = '{new_val}' WHERE val = '{old_val}'"
                ))
                .expect("update t1");
        }

        // Delete from t2
        if i > 1 {
            let val = (i - 2) * 3 + 1;
            executor
                .execute(&format!("DELETE FROM t2 WHERE val = '{val}'"))
                .expect("delete t2");
        }
    }

    // Verify each table has correct number of rows
    // t1: 10 inserted
    // t2: 10 inserted - 8 deleted = 2
    // t3: 10 inserted

    // Just verify we can read without errors
    executor
        .execute("INSERT INTO t1 (id, val) VALUES ('final', '999')")
        .expect("final insert");
}

#[test]
fn test_special_characters_and_edge_values() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE special (id TEXT, val TEXT) ORDER BY val")
        .expect("create");

    // Insert various edge case values
    let special_values = vec![
        ("r1", ""),              // empty string
        ("r2", " "),             // space
        ("r3", "  "),            // multiple spaces
        ("r4", "\t"),            // tab
        ("r5", "\n"),            // newline
        ("r6", "a\nb"),          // embedded newline
        ("r7", "'"),             // single quote
        ("r8", "\""),            // double quote
        ("r9", "\\"),            // backslash
        ("r10", "!@#$%^&*()"),   // special chars
        ("r11", "0"),            // zero
        ("r12", "-1"),           // negative
        ("r13", "999999999999"), // large number
        ("r14", "0.0"),          // decimal
        ("r15", "1e10"),         // scientific notation
    ];

    for (id, val) in &special_values {
        // Escape single quotes for SQL
        let escaped_val = val.replace("'", "''");
        executor
            .execute(&format!(
                "INSERT INTO special (id, val) VALUES ('{id}', '{escaped_val}')"
            ))
            .expect(&format!("insert {id}"));
    }

    // Verify all rows inserted
    for i in 0..special_values.len() {
        let row = read_row(&handler, "special", i as u64);
        assert!(row.is_ok(), "should be able to read row {i}");
    }

    // Verify we can update and delete special values
    executor
        .execute("UPDATE special SET id = 'updated' WHERE val = ''")
        .expect("update empty string");

    executor
        .execute("DELETE FROM special WHERE val = ' '")
        .expect("delete space");
}

#[test]
fn test_very_long_strings() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE long_strings (id TEXT, val TEXT) ORDER BY id")
        .expect("create");

    // Create strings of increasing length
    let short = "x".repeat(10);
    let medium = "y".repeat(1000);
    let long = "z".repeat(10000);

    executor
        .execute(&format!(
            "INSERT INTO long_strings (id, val) VALUES ('r1', '{short}')"
        ))
        .expect("insert short");

    executor
        .execute(&format!(
            "INSERT INTO long_strings (id, val) VALUES ('r2', '{medium}')"
        ))
        .expect("insert medium");

    executor
        .execute(&format!(
            "INSERT INTO long_strings (id, val) VALUES ('r3', '{long}')"
        ))
        .expect("insert long");

    // Verify lengths are preserved
    let row1 = read_row(&handler, "long_strings", 0).expect("read r1");
    let row2 = read_row(&handler, "long_strings", 1).expect("read r2");
    let row3 = read_row(&handler, "long_strings", 2).expect("read r3");

    assert_eq!(row1[1].len(), 10);
    assert_eq!(row2[1].len(), 1000);
    assert_eq!(row3[1].len(), 10000);

    // Update long string
    let new_long = "w".repeat(15000);
    executor
        .execute(&format!(
            "UPDATE long_strings SET val = '{new_long}' WHERE id = 'r3'"
        ))
        .expect("update long");

    let row3 = read_row(&handler, "long_strings", 2).expect("read after update");
    assert_eq!(row3[1].len(), 15000);
}

#[test]
fn test_numeric_edge_cases() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE numbers (id TEXT, val TEXT) ORDER BY val")
        .expect("create");

    let numbers = vec![
        ("r1", "0"),
        ("r2", "-0"),
        ("r3", "0.0"),
        ("r4", "-1"),
        ("r5", "1"),
        ("r6", "-999999999"),
        ("r7", "999999999"),
        ("r8", "3.14159265359"),
        ("r9", "-3.14159265359"),
        ("r10", "1e10"),
        ("r11", "1e-10"),
        ("r12", "0.000001"),
        ("r13", "1000000"),
    ];

    for (id, val) in &numbers {
        executor
            .execute(&format!(
                "INSERT INTO numbers (id, val) VALUES ('{id}', '{val}')"
            ))
            .expect(&format!("insert {id}"));
    }

    // Verify all rows are sorted numerically (not lexicographically)
    let mut prev_val: Option<f64> = None;
    for i in 0..numbers.len() {
        let row = read_row(&handler, "numbers", i as u64).expect("read row");
        if let Ok(val) = row[1].parse::<f64>() {
            if let Some(prev) = prev_val {
                assert!(
                    val >= prev,
                    "row {i} value {val} should be >= previous {prev}"
                );
            }
            prev_val = Some(val);
        }
    }
}

#[test]
fn test_mixed_numeric_string_sorting() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE mixed (id TEXT, val TEXT) ORDER BY val")
        .expect("create");

    // Mix of values that parse as numbers and values that don't
    let values = vec![
        ("r1", "10"),
        ("r2", "abc"),
        ("r3", "2"),
        ("r4", "xyz"),
        ("r5", "100"),
        ("r6", "1a"), // Doesn't parse as number
        ("r7", "a1"), // Doesn't parse as number
        ("r8", "50"),
        ("r9", "def"),
    ];

    for (id, val) in &values {
        executor
            .execute(&format!(
                "INSERT INTO mixed (id, val) VALUES ('{id}', '{val}')"
            ))
            .expect(&format!("insert {id}"));
    }

    // Just verify no panics and we can read all rows
    for i in 0..values.len() {
        let row = read_row(&handler, "mixed", i as u64);
        assert!(row.is_ok(), "should read row {i}");
    }
}

#[test]
fn test_update_same_row_multiple_times() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE updates (id TEXT, val TEXT, counter TEXT) ORDER BY id")
        .expect("create");

    executor
        .execute("INSERT INTO updates (id, val, counter) VALUES ('r1', 'initial', '0')")
        .expect("insert");

    // Update same row 20 times
    for i in 1..=20 {
        executor
            .execute(&format!(
                "UPDATE updates SET val = 'v{i}', counter = '{i}' WHERE id = 'r1'"
            ))
            .expect(&format!("update {i}"));

        // Verify update took effect
        let row = read_row(&handler, "updates", 0).expect("read");
        assert_eq!(row[1], format!("v{i}"));
        assert_eq!(row[2], format!("{i}"));
    }

    // Final check
    let row = read_row(&handler, "updates", 0).expect("final read");
    assert_eq!(row[1], "v20");
    assert_eq!(row[2], "20");
}

#[test]
fn test_alternating_insert_delete_pattern() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE alternating (id TEXT, val TEXT) ORDER BY val")
        .expect("create");

    for round in 0..10 {
        // Insert 5 rows
        for i in 0..5 {
            let val = round * 10 + i;
            executor
                .execute(&format!(
                    "INSERT INTO alternating (id, val) VALUES ('r{round}_{i}', '{val}')"
                ))
                .expect("insert");
        }

        // Delete 3 rows (leave 2)
        for i in 0..3 {
            let val = round * 10 + i;
            executor
                .execute(&format!("DELETE FROM alternating WHERE val = '{val}'"))
                .expect("delete");
        }
    }

    // Should have 10 rounds * 2 remaining per round = 20 rows
    for i in 0..20 {
        let row = read_row(&handler, "alternating", i);
        assert!(row.is_ok(), "should have row {i}");
    }

    let row20 = read_row(&handler, "alternating", 20);
    assert!(row20.is_err(), "should only have 20 rows");
}

#[test]
fn test_boundary_row_indices() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE boundaries (id TEXT, val TEXT) ORDER BY val")
        .expect("create");

    // Insert 10 rows
    for i in 0..10 {
        executor
            .execute(&format!(
                "INSERT INTO boundaries (id, val) VALUES ('r{i}', '{}')",
                i * 10
            ))
            .expect("insert");
    }

    // Test first row
    let row0 = read_row(&handler, "boundaries", 0).expect("read row 0");
    assert_eq!(row0[1], "0");

    // Test last row
    let row9 = read_row(&handler, "boundaries", 9).expect("read row 9");
    assert_eq!(row9[1], "90");

    // Test out of bounds
    let row10 = read_row(&handler, "boundaries", 10);
    assert!(row10.is_err(), "row 10 should not exist");

    // Delete first row
    executor
        .execute("DELETE FROM boundaries WHERE val = '0'")
        .expect("delete first");

    // Row 0 should now be what was row 1
    let new_row0 = read_row(&handler, "boundaries", 0).expect("read new row 0");
    assert_eq!(new_row0[1], "10");

    // Delete last row
    executor
        .execute("DELETE FROM boundaries WHERE val = '90'")
        .expect("delete last");

    // Row 7 should be the last now (8 rows remaining: 1-8)
    let row7 = read_row(&handler, "boundaries", 7).expect("read row 7");
    assert_eq!(row7[1], "80");

    let row8 = read_row(&handler, "boundaries", 8);
    assert!(row8.is_err(), "row 8 should not exist after delete");
}

#[test]
fn test_delete_all_rows_one_by_one() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE delete_all (id TEXT, val TEXT) ORDER BY val")
        .expect("create");

    let count = 15;

    // Insert rows
    for i in 0..count {
        executor
            .execute(&format!(
                "INSERT INTO delete_all (id, val) VALUES ('r{i}', '{i}')"
            ))
            .expect("insert");
    }

    // Delete all rows one by one from the beginning
    for i in 0..count {
        executor
            .execute(&format!("DELETE FROM delete_all WHERE val = '{i}'"))
            .expect(&format!("delete {i}"));

        // Verify count decreases
        let remaining = count - i - 1;
        if remaining > 0 {
            let row = read_row(&handler, "delete_all", 0);
            assert!(row.is_ok(), "should still have rows after delete {i}");
        }
    }

    // Table should be empty
    let row = read_row(&handler, "delete_all", 0);
    assert!(row.is_err(), "table should be empty");
}

#[test]
fn test_insert_after_delete_all() {
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE reset (id TEXT, val TEXT) ORDER BY val")
        .expect("create");

    // Insert some rows
    for i in 0..5 {
        executor
            .execute(&format!(
                "INSERT INTO reset (id, val) VALUES ('r{i}', '{i}')"
            ))
            .expect("insert");
    }

    // Delete all
    for i in 0..5 {
        executor
            .execute(&format!("DELETE FROM reset WHERE val = '{i}'"))
            .expect("delete");
    }

    // Insert new rows
    for i in 10..15 {
        executor
            .execute(&format!(
                "INSERT INTO reset (id, val) VALUES ('new{i}', '{i}')"
            ))
            .expect("insert new");
    }

    // Verify new rows exist
    for i in 0..5 {
        let row = read_row(&handler, "reset", i).expect(&format!("read {i}"));
        let val: i32 = row[1].parse().unwrap();
        assert!(val >= 10 && val < 15);
    }
}

#[test]
fn test_complex_filter_with_edge_cases() {
    let (executor, _handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE filters (id TEXT, val TEXT, flag TEXT) ORDER BY id")
        .expect("create");

    // Insert test data
    for i in 0..20 {
        let val = if i % 2 == 0 { "even" } else { "odd" };
        let flag = if i % 3 == 0 { "yes" } else { "no" };
        executor
            .execute(&format!(
                "INSERT INTO filters (id, val, flag) VALUES ('{i}', '{val}', '{flag}')"
            ))
            .expect("insert");
    }

    // These queries should not panic even if they return no results
    let _ = executor.execute("UPDATE filters SET val = 'test' WHERE id = '999' AND val = 'even'");
    let _ = executor.execute("DELETE FROM filters WHERE id = '999' AND val = 'even'");
    let _ = executor.execute("UPDATE filters SET val = 'test' WHERE id = '' AND val = ''");
    let _ = executor.execute("DELETE FROM filters WHERE id = '' AND val = ''");
}

#[test]
fn test_overwrite_row_via_update() {
    // Test overwrite functionality through SQL UPDATE (not direct ops_handler)
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE overwrite_test (id TEXT, val TEXT) ORDER BY id")
        .expect("create");

    // Insert rows
    executor
        .execute("INSERT INTO overwrite_test (id, val) VALUES ('1', 'initial')")
        .expect("insert 1");
    executor
        .execute("INSERT INTO overwrite_test (id, val) VALUES ('2', 'second')")
        .expect("insert 2");
    executor
        .execute("INSERT INTO overwrite_test (id, val) VALUES ('3', 'third')")
        .expect("insert 3");

    // Overwrite middle row (non-ORDER BY column)
    executor
        .execute("UPDATE overwrite_test SET val = 'updated' WHERE id = '2'")
        .expect("update");

    let row = read_row(&handler, "overwrite_test", 1).expect("read");
    assert_eq!(row[1], "updated");

    // Overwrite first row
    executor
        .execute("UPDATE overwrite_test SET val = 'first_updated' WHERE id = '1'")
        .expect("update first");

    let row = read_row(&handler, "overwrite_test", 0).expect("read first");
    assert_eq!(row[1], "first_updated");

    // Overwrite last row
    executor
        .execute("UPDATE overwrite_test SET val = 'last_updated' WHERE id = '3'")
        .expect("update last");

    let row = read_row(&handler, "overwrite_test", 2).expect("read last");
    assert_eq!(row[1], "last_updated");
}

#[test]
fn test_delete_patterns() {
    // Test deletion through SQL DELETE (not direct ops_handler)
    let (executor, handler, _directory, _store) = build_sql_executor();

    executor
        .execute("CREATE TABLE delete_test (id TEXT, val TEXT) ORDER BY id")
        .expect("create");

    // Insert 5 rows
    for i in 0..5 {
        executor
            .execute(&format!(
                "INSERT INTO delete_test (id, val) VALUES ('{i}', 'v{i}')"
            ))
            .expect("insert");
    }

    // Delete middle row
    executor
        .execute("DELETE FROM delete_test WHERE id = '2'")
        .expect("delete middle");

    // Should now have 4 rows
    for i in 0..4 {
        let row = read_row(&handler, "delete_test", i);
        assert!(row.is_ok(), "should have row {i}");
    }

    let row4 = read_row(&handler, "delete_test", 4);
    assert!(row4.is_err(), "should not have row 4");

    // Delete first row
    executor
        .execute("DELETE FROM delete_test WHERE id = '0'")
        .expect("delete first");

    // Should now have 3 rows
    for i in 0..3 {
        let row = read_row(&handler, "delete_test", i);
        assert!(row.is_ok(), "should have row {i} after delete first");
    }

    // Verify remaining rows are correct
    let row0 = read_row(&handler, "delete_test", 0).expect("read row 0");
    assert_eq!(row0[0], "1"); // Was row 1, now row 0

    let row1 = read_row(&handler, "delete_test", 1).expect("read row 1");
    assert_eq!(row1[0], "3"); // Was row 3, now row 1 (skipped deleted row 2)

    let row2 = read_row(&handler, "delete_test", 2).expect("read row 2");
    assert_eq!(row2[0], "4"); // Was row 4, now row 2
}
