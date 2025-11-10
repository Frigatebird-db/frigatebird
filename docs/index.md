---
layout: default
title: "Home"
nav_order: 1
---

# Satori Documentation

Comprehensive internal architecture documentation for Satori, a high-performance columnar database system.

---

## Getting Started

- [System Overview](system_overview) - Progressive deep-dive from 30,000ft to implementation
- [Architecture Overview](architecture_overview) - End-to-end system blueprint

---

## Core Components

### Storage Layer

- [Core Types](core_types) - Entry and Page data structures
- [Cache System](cache) - Two-tier LRU cache with lifecycle callbacks (UPC → CPC → Disk)
- [Storage Layer](storage) - Block allocation, file rotation, and disk I/O with io_uring

### Write Path

- [Writer](writer) - Write execution subsystem with block allocator

### Metadata & Catalog

- [Metadata Store](metadata_store) - Column version chains with prefix sums
- [Page Handler](page_handler) - Cache orchestration and page location

### Operations API

- [Operations Handler](ops_handler) - High-level upsert/update/range_scan API
- [Sorted Inserts](sorted_inserts) - ORDER BY table insertion strategies

---

## Query Processing

### SQL Pipeline

- [SQL Parser](sql_parser) - SQL tokenization and AST generation
- [Query Planner](query_planner) - Logical plan construction and optimization
- [Pipeline Builder](pipeline) - Filter-based execution pipeline construction
- [Pipeline Executor](executor) - Dual-pool work-stealing job execution
- [SQL Executor](sql_executor) - CREATE TABLE, INSERT, UPDATE, DELETE execution

### Runtime

- [Thread Pool Scheduler](scheduler) - Thread pool implementation

---

## Documentation Map

| Category | Documents | Status |
|----------|-----------|--------|
| **Overview** | system_overview, architecture_overview | Complete |
| **Storage** | core_types, cache, storage | Complete |
| **Write Path** | writer | Complete |
| **Metadata** | metadata_store, page_handler | Complete |
| **Operations** | ops_handler, sorted_inserts | Complete |
| **Query** | sql_parser, query_planner, pipeline, executor, sql_executor, scheduler | Complete |

---

## Quick Reference

### Key Constants

| Constant | Value | Location |
|----------|-------|----------|
| Cache Size (LRU) | 10 pages per tier | [cache](cache) |
| Block Size | 256 KiB | [storage](storage) |
| File Max Size | 4 GiB | [storage](storage) |
| Thread Split | 85% main / 15% reserve | [executor](executor) |

### Critical Paths

| Path | Entry Point | Documentation |
|------|-------------|---------------|
| **Write** | `Writer::submit()` | [writer](writer) |
| **Read** | `PageHandler::get_page()` | [page_handler](page_handler) |
| **Query (plan)** | `plan_sql()` | [query_planner](query_planner) |
| **SQL Exec** | `SqlExecutor::execute()` | [sql_executor](sql_executor) |
| **Allocation** | `allocator.allocate()` | [storage](storage) |

---

## Architecture Diagrams

All documentation includes ASCII diagrams for:
- Data flow through components
- Thread interaction patterns
- Memory layout and alignment
- State machines and protocols

---

## Notes

- All documentation reflects **actual implementation** (no future plans or hypotheticals)
- Constants are hardcoded (not runtime-configurable)
- Platform differences noted where applicable (Linux O_DIRECT, etc.)
