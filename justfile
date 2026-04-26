set shell := ["bash", "-c"]
publish_order := "tracen_ir tracen_analytics tracen_catalog tracen_dsl tracen_eval tracen_export tracen_pack_codegen tracen_engine tracen_pack tracen_ffi_core tracen_ffi tracen"

# Default target prints available recipes.
default:
	@just --list

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

check: fmt-check clippy test doc

publish-check:
	cargo check --workspace

publish-dry-run:
	@for pkg in {{publish_order}}; do \
		echo "==> dry-run $pkg"; \
		cargo publish -p "$pkg" --dry-run --no-verify; \
	done

publish-all:
	@for pkg in {{publish_order}}; do \
		echo "==> publish $pkg"; \
		attempt=1; \
		until cargo publish -p "$pkg"; do \
			if [ "$attempt" -ge 12 ]; then \
				echo "publish failed for $pkg after $attempt attempts"; \
				exit 1; \
			fi; \
			echo "retry $attempt for $pkg after index propagation wait"; \
			attempt=$((attempt + 1)); \
			sleep 15; \
		done; \
		sleep 10; \
	done

release-check: check publish-dry-run
