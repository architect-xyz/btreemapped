nightly := "nightly-2025-12-07"

# Run tests with coverage (lcov output)
coverage:
    cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info

# Run tests with coverage and open HTML report
coverage-html:
    cargo llvm-cov --all-features --workspace --html --open

# List available recipes
default:
    @just --list

# Restart postgres and run the basic example
example:
    docker compose down postgres
    docker compose up -d postgres
    cargo run --example basic -p btreemapped

# Auto-fix clippy warnings
fix:
    cargo clippy --all-features --workspace --fix --allow-dirty -- -D warnings

# Format all Rust and TOML files
format:
    cargo +{{nightly}} fmt --all
    taplo fmt

# Run clippy lints
lint:
    cargo clippy --all-features --workspace -- -D warnings

# Run tests like CI (all features, all workspace crates)
test:
    cargo test --all-features --workspace
