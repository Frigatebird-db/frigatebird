use std::sync::{Arc, RwLock};

use clap::Parser;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::Editor;

use frigatebird::cache::page_cache::PageCache;
use frigatebird::helpers::compressor::Compressor;
use frigatebird::metadata_store::{PageDirectory, TableMetaStore};
use frigatebird::page_handler::page_io::PageIO;
use frigatebird::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use frigatebird::sql::executor::SqlExecutor;

#[derive(Parser, Debug)]
#[command(name = "frigatebird", version, about = "Columnar SQL database with push-based Volcano execution")]
struct Args {
    /// Execute a single SQL statement and exit
    #[arg(short = 'c', long)]
    command: Option<String>,
}

const PROMPT: &str = "sql> ";
const RESET: &str = "\x1b[0m";
const BANNER_COLOR: &str = "\x1b[38;5;75m";
const DIM: &str = "\x1b[2m";

fn main() {
    let args = Args::parse();
    let executor = setup_executor();

    match args.command {
        Some(sql) => run_single(&executor, &sql),
        None => run_repl(executor),
    }
}

fn setup_executor() -> SqlExecutor {
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
    SqlExecutor::new(handler, directory)
}

fn run_single(executor: &SqlExecutor, sql: &str) {
    execute_sql(executor, sql);
}

fn run_repl(executor: SqlExecutor) {
    print_banner();
    println!("Type SQL or \\? for help. Ctrl+C to exit.\n");

    let mut editor = match Editor::<(), DefaultHistory>::new() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("failed to initialize readline: {e}");
            return;
        }
    };

    loop {
        match editor.readline(PROMPT) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let _ = editor.add_history_entry(trimmed);

                if let Some(cmd) = trimmed.strip_prefix('\\') {
                    handle_backslash_command(&executor, cmd);
                    continue;
                }

                if let Some(cmd) = trimmed.strip_prefix('.') {
                    handle_dot_command(&executor, cmd);
                    continue;
                }

                execute_sql(&executor, trimmed);
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                println!();
                break;
            }
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        }
    }
}

fn execute_sql(executor: &SqlExecutor, sql: &str) {
    let upper = sql.trim().to_uppercase();

    if upper.starts_with("SELECT") {
        match executor.query(sql) {
            Ok(result) => print!("{result}"),
            Err(e) => eprintln!("error: {e}"),
        }
    } else {
        match executor.execute(sql) {
            Ok(()) => println!("OK"),
            Err(e) => eprintln!("error: {e}"),
        }
    }
}

fn handle_backslash_command(executor: &SqlExecutor, cmd: &str) {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let command = parts.first().copied().unwrap_or("");

    match command {
        "?" | "h" | "help" => print_help(),
        "d" => {
            if let Some(table) = parts.get(1) {
                describe_table(executor, table);
            } else {
                list_tables(executor);
            }
        }
        "dt" => list_tables(executor),
        "d+" => {
            if let Some(table) = parts.get(1) {
                describe_table(executor, table);
            } else {
                println!("usage: \\d+ <table_name>");
            }
        }
        "q" => std::process::exit(0),
        "i" => {
            if let Some(path) = parts.get(1) {
                execute_file(executor, path);
            } else {
                println!("usage: \\i <filename>");
            }
        }
        "" => {}
        _ => {
            println!("unknown command: \\{command}");
            println!("type \\? for help");
        }
    }
}

fn handle_dot_command(executor: &SqlExecutor, cmd: &str) {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let command = parts.first().copied().unwrap_or("");

    match command {
        "help" | "h" => print_help(),
        "tables" => list_tables(executor),
        "schema" => {
            if let Some(table) = parts.get(1) {
                describe_table(executor, table);
            } else {
                println!("usage: .schema <table_name>");
            }
        }
        "exit" | "quit" | "q" => {
            std::process::exit(0);
        }
        _ => {
            println!("unknown command: .{command}");
            println!("type \\? for help");
        }
    }
}

fn print_help() {
    println!("Commands:");
    println!("  \\d             List all tables");
    println!("  \\d <table>     Describe table columns");
    println!("  \\dt            List all tables");
    println!("  \\i <file>      Execute SQL from file");
    println!("  \\q             Quit");
    println!();
    println!("{DIM}SQL: CREATE TABLE ... ORDER BY, INSERT, SELECT, UPDATE, DELETE{RESET}");
}

fn list_tables(executor: &SqlExecutor) {
    let tables = executor.table_names();
    if tables.is_empty() {
        println!("(no tables)");
    } else {
        for name in tables {
            println!("  {name}");
        }
    }
}

fn describe_table(executor: &SqlExecutor, table: &str) {
    match executor.get_table_catalog(table) {
        Some(catalog) => {
            println!("Table: {}", table);
            println!("{:-<40}", "");
            for col in catalog.columns() {
                println!("  {:<20} {:?}", col.name, col.data_type);
            }
        }
        None => println!("table '{}' not found", table),
    }
}

fn execute_file(executor: &SqlExecutor, path: &str) {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            for line in contents.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with("--") {
                    continue;
                }
                println!("{DIM}>{RESET} {trimmed}");
                execute_sql(executor, trimmed);
            }
        }
        Err(e) => eprintln!("error reading file: {e}"),
    }
}

fn print_banner() {
    for line in FRIGATEBIRD_ASCII.lines() {
        println!("{BANNER_COLOR}{line}{RESET}");
    }
    println!();
}

const FRIGATEBIRD_ASCII: &str = r#"
 _____ ____  ___ ____    _  _____ _____ ____ ___ ____  ____
|  ___|  _ \|_ _/ ___|  / \|_   _| ____| __ )_ _|  _ \|  _ \
| |_  | |_) || | |  _  / _ \ | | |  _| |  _ \| || |_) | | | |
|  _| |  _ < | | |_| |/ ___ \| | | |___| |_) | ||  _ <| |_| |
|_|   |_| \_\___\____/_/   \_\_| |_____|____/___|_| \_\____/
"#;
