.PHONY: test frigatebird

test:
	@set -e; \
	cargo test --release ; \
	rm -rf wal_files

frigatebird:
	WALRUS_QUIET=1 cargo run --bin frigatebird
