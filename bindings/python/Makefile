REQUIREMENTS     := requirements.txt
REQUIREMENTS_DEV := requirements-dev.txt

all: check-requirements install lint test
.PHONY: all

install:
	@echo "Installing requirements..."
	pip install -r requirements.txt -r requirements-dev.txt
.PHONY: install

test:
	@echo "Running tests..."
	pytest
.PHONY: test

lint:
	@echo "Running linters..."
	ruff check
	ruff format --diff
.PHONY: lint

check-requirements:
	@echo "Checking requirements files..."
	mkdir -p .tmp
	pip-compile pyproject.toml --quiet             --output-file=.tmp/$(REQUIREMENTS)
	pip-compile pyproject.toml --quiet --extra=dev --output-file=.tmp/$(REQUIREMENTS_DEV)
	diff -u $(REQUIREMENTS)     .tmp/$(REQUIREMENTS)     || (echo "$(REQUIREMENTS) doesn't match pyproject.toml"     && exit 1)
	diff -u $(REQUIREMENTS_DEV) .tmp/$(REQUIREMENTS_DEV) || (echo "$(REQUIREMENTS_DEV) doesn't match pyproject.toml" && exit 1)
	@echo "Requirements files match pyproject.toml"
.PHONY: check-requirements

compile-requirements:
	@echo "Compiling requirements files..."
	pip-compile pyproject.toml             --output-file=$(REQUIREMENTS)
	pip-compile pyproject.toml --extra=dev --output-file=$(REQUIREMENTS_DEV)
.PHONY: compile-requirements
