.PHONY: lib

run_test: build_test
	./gradlew test

build_test:
	CARGO_TARGET_DIR=src/test/resources/limbo cargo build
