all:
	cargo build
	cargo build --package limbo-wasm --target wasm32-wasi
.PHONY: all
