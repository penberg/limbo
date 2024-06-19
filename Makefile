all:
	cargo build
	cargo build --package limbo-wasm --target wasm32-wasi
.PHONY: all

test: all
	SQLITE_EXEC=./target/debug/limbo ./testing/all.test
.PHONY: test
