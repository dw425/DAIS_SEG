# Contributing

Thank you for your interest in contributing to the Universal ETL Migration Platform.

## Getting Started

1. Fork the repository
2. Clone your fork
3. Create a feature branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Run tests (see below)
6. Commit with a descriptive message
7. Push to your fork and open a Pull Request

## Development Setup

### Backend

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -e ".[all]"
```

### Frontend

```bash
cd frontend
npm install
```

## Running Tests

### Backend

```bash
cd backend
python -m pytest tests/ -q
```

### Frontend

```bash
cd frontend
npm test
npm run lint
npx tsc --noEmit
```

## Code Style

### Python (Backend)

- Formatter/linter: [Ruff](https://docs.astral.sh/ruff/)
- Line length: 120
- Target: Python 3.11+
- Type hints required for function signatures

### TypeScript (Frontend)

- Strict TypeScript with `noEmit` checks
- ESLint for linting
- Tailwind CSS for styling (no custom CSS files)

## Adding a New ETL Platform Parser

1. Create `backend/app/engines/parsers/your_platform.py`
2. Implement `parse_your_platform(content: bytes, filename: str) -> ParseResult`
3. Register in `backend/app/engines/parsers/dispatcher.py`:
   - Add extension mapping in `_EXT_MAP`
   - Add content detection in the appropriate `_detect_*` function
   - Add parser function in `_dispatch()`
4. Add tests in `backend/tests/`

## Adding a New Lint Rule

1. Open `backend/app/constants/lint_rules.yaml`
2. Add a new rule entry with: `id`, `name`, `severity`, `category`, `description`, `suggestion`
3. Implement the check function in `backend/app/engines/analyzers/flow_linter.py`
4. Register in `_CHECK_FUNCTIONS` dict

## Pull Request Guidelines

- Keep PRs focused on a single change
- Include tests for new functionality
- Update types in `frontend/src/types/pipeline.ts` if the API response shape changes
- Backend and frontend changes that go together should be in the same PR

## Reporting Issues

Please use GitHub Issues and include:
- Steps to reproduce
- Expected vs actual behavior
- ETL platform and file format (if applicable)
- Browser and OS version (for frontend issues)
