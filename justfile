# List available recipes
default:
    @just --list

# Run tests with coverage (lcov output)
coverage:
    cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info

# Run tests with coverage and open HTML report
coverage-html:
    cargo llvm-cov --all-features --workspace --html --open

# Restart postgres and run the basic example
example:
    docker compose down postgres
    docker compose up -d postgres
    cargo run --example basic -p btreemapped
