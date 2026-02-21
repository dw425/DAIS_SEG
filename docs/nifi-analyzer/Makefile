.PHONY: dev dev-backend dev-frontend build test lint clean install

# Development
dev: dev-backend dev-frontend

dev-backend:
	cd backend && uvicorn app.main:app --reload --port 8000

dev-frontend:
	cd frontend && npm run dev

# Install dependencies
install:
	cd backend && pip install -e ".[dev]"
	cd frontend && npm install

# Build
build:
	cd frontend && npm run build
	@echo "Frontend built to frontend/dist/"

# Test
test: test-backend test-frontend

test-backend:
	cd backend && python -m pytest tests/ -v

test-frontend:
	cd frontend && npm test

# Lint
lint: lint-backend lint-frontend

lint-backend:
	cd backend && ruff check app/ tests/

lint-frontend:
	cd frontend && npm run lint

# Docker
docker-up:
	docker-compose up --build

docker-down:
	docker-compose down

# Clean
clean:
	rm -rf frontend/dist frontend/node_modules backend/__pycache__
	find backend -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
