.PHONY: java_lint test build_test

lint:
	./gradlew spotlessCheck

lint_apply:
	./gradlew spotlessApply

test: lint build_test
	./gradlew test --info

build_test:
	CARGO_TARGET_DIR=src/test/resources/limbo cargo build
