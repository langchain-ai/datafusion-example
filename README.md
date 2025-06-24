# DataFusion Query Runner

A Rust application demonstrating DataFusion query execution with parquet files.

## Prerequisites

- Rust (latest stable version)
- Cargo

## Quick Start

1. **Build and run in release mode**:
    ```bash
   cargo run --release --bin query_runner
    ```

The application will:
- Load the parquet file from `data/` directory
- Execute a point query
- Show logical and physical query plans

## Project Structure

- `src/main.rs` - Main application code
- `data/` - Contains the parquet data file
- `Cargo.toml` - Project dependencies and configuration