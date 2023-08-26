all:
	cargo build
	cargo build --package lig-wasm --target wasm32-wasi
.PHONY: all
