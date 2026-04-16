# Crypto Lake RS

## Description
Lightweight crypto data collector and transformer written in Rust. Connects to cryptocurrency exchanges via WebSocket, processes data into Arrow/Parquet columnar format with Zstandard compression.

## App Type
Backend Service / CLI Tool / Data Pipeline

## Existing Versions
- CLI (cross-platform)
- Windows system tray integration

## Tech Stack
- Rust (2021 edition)
- Tokio (async runtime)
- Tokio-tungstenite (WebSocket client)
- Serde (JSON/YAML serialization)
- Arrow / Parquet (columnar data format)
- Zstd (compression)
- Axum (HTTP/WebSocket server)
- Tower (middleware, CORS)
- Tracing (structured logging with JSON output)
- Clap (CLI arguments)
- Chrono (date/time)
- tray-icon (Windows system tray)

## Working Functions/Features
- WebSocket client for real-time exchange data
- Arrow/Parquet data format output
- Zstandard compression
- Async/concurrent processing
- HTTP/WebSocket server with CORS
- JSON and YAML serialisation
- CLI argument parsing
- Structured logging with JSON output
- System tray integration (Windows only)
- Release-optimised binary (LTO, strip, single codegen unit)
- Event handling system
- Data transformation pipeline
- Resource cleanup

## Entry Points
- Binary compiled from `/src/`
- `/src/events.rs` - Event handling
- `/src/cleanup.rs` - Resource cleanup
- `/src/transformer/mod.rs` - Data transformation

## Build
- `cargo build --release` - Optimised production build
