.PHONY: test

test:
	@set -e; \
	cargo test --release ; \
	rm -rf wal_files
