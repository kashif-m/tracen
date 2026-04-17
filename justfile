set shell := ["bash", "-c"]

# Default target prints available recipes.
default:
	@just --list

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

check: fmt clippy test doc

publish-check:
	cargo check --workspace
