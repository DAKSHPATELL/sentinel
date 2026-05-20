.PHONY: setup start stop status test lint clean

setup:
	@echo "=== SENTINEL Setup ==="
	pip install -e ".[dev,full]"
	docker compose pull
	docker compose up -d
	python -m spacy download en_core_web_trf || true
	playwright install chromium || true
	@echo "=== Setup Complete ==="

start:
	docker compose up -d
	sentinel start

stop:
	sentinel stop
	docker compose stop

status:
	sentinel status
	docker compose ps

test:
	python -m pytest tests/ -v --tb=short

lint:
	ruff check sentinel/ tests/
	ruff format --check sentinel/ tests/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .ruff_cache .mypy_cache
	rm -rf data/cache/*
