MINIMUM_RUST_VERSION := 1.73.0
CURRENT_RUST_VERSION := $(shell rustc -V | sed -E 's/rustc ([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
CURRENT_RUST_TARGET := $(shell rustc -vV | grep host | cut -d ' ' -f 2)
RUSTUP := $(shell command -v rustup 2> /dev/null)
UNAME_S := $(shell uname -s)

# Executable used to execute the compatibility tests.
SQLITE_EXEC ?= ./target/debug/limbo

# Static library to use for SQLite C API compatibility tests.
BASE_SQLITE_LIB = ./target/$(CURRENT_RUST_TARGET)/debug/liblimbo_sqlite3.a
# Static library headers to use for SQLITE C API compatibility tests
BASE_SQLITE_LIB_HEADERS = ./target/$(CURRENT_RUST_TARGET)/debug/include/limbo_sqlite3


# On darwin link core foundation
ifeq ($(UNAME_S),Darwin)
    SQLITE_LIB ?= ../../$(BASE_SQLITE_LIB) -framework CoreFoundation
else
    SQLITE_LIB ?= ../../$(BASE_SQLITE_LIB)
endif

SQLITE_LIB_HEADERS ?= ../../$(BASE_SQLITE_LIB_HEADERS)

all: check-rust-version check-wasm-target limbo limbo-wasm
.PHONY: all

check-rust-version:
	@echo "Checking Rust version..."
	@if [ "$(shell printf '%s\n' "$(MINIMUM_RUST_VERSION)" "$(CURRENT_RUST_VERSION)" | sort -V | head -n1)" = "$(CURRENT_RUST_VERSION)" ]; then \
		echo "Rust version greater than $(MINIMUM_RUST_VERSION) is required. Current version is $(CURRENT_RUST_VERSION)."; \
		if [ -n "$(RUSTUP)" ]; then \
			echo "Updating Rust..."; \
			rustup update stable; \
		else \
			echo "Please update Rust manually to a version greater than $(MINIMUM_RUST_VERSION)."; \
			exit 1; \
		fi; \
	else \
		echo "Rust version $(CURRENT_RUST_VERSION) is acceptable."; \
	fi
.PHONY: check-rust-version

check-wasm-target:
	@echo "Checking wasm32-wasi target..."
	@if ! rustup target list | grep -q "wasm32-wasi (installed)"; then \
		echo "Installing wasm32-wasi target..."; \
		rustup target add wasm32-wasi; \
	fi
.PHONY: check-wasm-target

limbo:
	cargo build
.PHONY: limbo

limbo-c:
	cargo cbuild
.PHONY: limbo-c

limbo-wasm:
	rustup target add wasm32-wasi
	cargo build --package limbo-wasm --target wasm32-wasi
.PHONY: limbo-wasm

test: limbo test-compat test-vector test-sqlite3 test-shell test-extensions
.PHONY: test

test-extensions: limbo
	cargo build --package limbo_regexp
	./testing/extensions.py
.PHONY: test-extensions

test-shell: limbo 
	SQLITE_EXEC=$(SQLITE_EXEC) ./testing/shelltests.py
.PHONY: test-shell

test-compat:
	SQLITE_EXEC=$(SQLITE_EXEC) ./testing/all.test
.PHONY: test-compat

test-vector:
	SQLITE_EXEC=$(SQLITE_EXEC) ./testing/vector.test
.PHONY: test-vector

test-time:
	SQLITE_EXEC=$(SQLITE_EXEC) ./testing/time.test
.PHONY: test-time

test-sqlite3: limbo-c
	LIBS="$(SQLITE_LIB)" HEADERS="$(SQLITE_LIB_HEADERS)" make -C sqlite3/tests test
.PHONY: test-sqlite3

test-json:
	SQLITE_EXEC=$(SQLITE_EXEC) ./testing/json.test
.PHONY: test-json
