.PHONY: build clean test run

build:
	uv sync
	uv build --refresh --wheel

clean:
	@echo "Removing build and test artifacts..."
	@rm -rf dist/ build/ *.egg-info/ src/*.egg-info/ .pytest_cache/ .venv/ target/ tmp/ metastore_db/ derby.log spark-warehouse/

test:
	uv run --group test pytest -v
