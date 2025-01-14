.PHONY: test build_test

test: build_test
	./gradlew test

build_test:
	CARGO_TARGET_DIR=src/test/resources/limbo cargo build
