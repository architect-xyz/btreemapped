# List available recipes
default:
    @just --list

# Restart postgres and run the basic example
example:
    docker compose down postgres
    docker compose up -d postgres
    cargo run --example basic -p btreemapped
