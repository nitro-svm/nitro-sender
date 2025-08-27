set unstable := true

# Check for unused dependencies from Cargo.toml
[group('lint')]
check-udeps:
    cargo +nightly udeps

[group('lint')]
[private]
fmt-justfile:
    just --fmt --check

# Run lint and formatting checks for the entire project
[group('lint')]
lint: fmt-justfile
    cargo +nightly fmt -- --check
    cargo clippy --all-targets --all-features
    zepter

[group('lint')]
[private]
fmt-justfile-fix:
    just --fmt

# Fix lint and formatting issues in the entire project
[group('lint')]
lint-fix: fmt-justfile-fix
    cargo +nightly fmt
    cargo clippy --fix --allow-dirty --allow-staged --all-targets --all-features
    zepter

# Run tests for the crates in the workspace
[group('test')]
test:
    cargo nextest run --workspace

# Build the crate for release
[group('build')]
build:
    cargo build --release
