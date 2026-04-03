.PHONY: setup serve test lint collect deploy

setup:
	uv sync --all-extras
	mkdir -p data
	cp -n .env.example .env 2>/dev/null || true

serve:
	uv run uvicorn app.main:app --host 127.0.0.1 --port 8003 --reload

test:
	uv run pytest -x -q

lint:
	uvx ruff check app/
	uvx ruff format --check app/

collect:
	uv run python -m app.cli collect-all

deploy:
	bash deploy.sh
