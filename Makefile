.PHONY: test

test:
	@set -e; \
	cargo test; \
	cargo test --doc; \
	rm -rf wal_files
