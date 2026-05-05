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

bump arg1="up" arg2="patch":
	@set -euo pipefail; \
	mode="{{arg1}}"; \
	level="{{arg2}}"; \
	if [ "$mode" != "up" ] && [ "$mode" != "down" ]; then \
		level="$mode"; \
		mode="up"; \
	fi; \
	case "$mode" in up|down) ;; *) echo "mode must be 'up' or 'down'"; exit 2;; esac; \
	case "$level" in patch|minor|major) ;; *) echo "level must be 'patch', 'minor', or 'major'"; exit 2;; esac; \
	if ! git diff --quiet -- Cargo.toml Cargo.lock || ! git diff --cached --quiet -- Cargo.toml Cargo.lock; then \
		echo "commit or stash existing Cargo.toml/Cargo.lock changes before bumping"; \
		exit 2; \
	fi; \
	old="$(awk '/^\[workspace.package\]$/ { in_section = 1; next } /^\[/ { in_section = 0 } in_section && /^version = / { gsub(/"/, "", $3); print $3; exit }' Cargo.toml)"; \
	if [ -z "$old" ]; then echo "Cargo.toml is missing [workspace.package] version"; exit 2; fi; \
	IFS=. read -r major minor patch <<< "$old"; \
	case "$major.$minor.$patch" in *[!0-9.]*|*.*.*.*) echo "unsupported semver version: $old"; exit 2;; esac; \
	if [ "$mode" = "up" ]; then \
		case "$level" in \
			major) major=$((major + 1)); minor=0; patch=0;; \
			minor) minor=$((minor + 1)); patch=0;; \
			patch) patch=$((patch + 1));; \
		esac; \
	else \
		case "$level" in \
			major) major=$((major - 1));; \
			minor) minor=$((minor - 1));; \
			patch) patch=$((patch - 1));; \
		esac; \
	fi; \
	if [ "$major" -lt 0 ] || [ "$minor" -lt 0 ] || [ "$patch" -lt 0 ]; then \
		echo "cannot bump $level $mode from $old"; \
		exit 2; \
	fi; \
	new="$major.$minor.$patch"; \
	packages="{{publish_order}}"; \
	tmp="$(mktemp)"; \
	awk -v old="$old" -v new="$new" -v packages="$packages" '\
		BEGIN { split(packages, names, " "); for (i in names) package[names[i]] = 1 } \
		/^\[workspace.package\]$/ { in_workspace_package = 1; print; next } \
		/^\[/ && $0 != "[workspace.package]" { in_workspace_package = 0 } \
		in_workspace_package && $0 == "version = \"" old "\"" { print "version = \"" new "\""; next } \
		{ \
			line = $0; \
			for (name in package) { \
				prefix = name " = { version = \"" old "\""; \
				if (index(line, prefix) == 1) { sub("version = \"" old "\"", "version = \"" new "\"", line); break } \
			} \
			print line; \
		}' Cargo.toml > "$tmp"; \
	mv "$tmp" Cargo.toml; \
	cargo check --workspace --offline >/dev/null; \
	git add Cargo.toml Cargo.lock; \
	git commit -m "chore: bump $mode $level version" >/dev/null; \
	echo "tracen workspace version: $old -> $new"

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
