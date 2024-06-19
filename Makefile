all: limbo limbo-wasm
.PHONY: all

limbo:
	cargo build
.PHONY: limbo

limbo-wasm:
	cargo build --package limbo-wasm --target wasm32-wasi
.PHONY: limbo-wasm

test: limbo
	SQLITE_EXEC=./target/debug/limbo ./testing/all.test
.PHONY: test
