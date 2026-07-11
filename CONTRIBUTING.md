# Contributing

## Development setup

```bash
git clone https://github.com/srchilukoori/tollkeeper.git
cd tollkeeper
uv sync
```

Run the test suite:

```bash
uv run pytest tests/ -v
```

Run lint and format checks:

```bash
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

Type check:

```bash
uv run ty
```

All of the above must pass before you open a PR. `uv run ruff format src/ tests/ && uv run ruff check src/ tests/ && uv run pytest tests/ -v` runs everything except the type checker in one shot.

Use `uv` for everything. Do not use `pip` or `venv` directly.

## Before submitting a PR

- Search open and closed PRs for the same problem. Don't duplicate an existing effort — comment on it instead.
- One problem per PR. Bundle unrelated changes and it gets closed.
- Target `develop`, not `main`. Branch names: `feature/<name>` for features, `fix/<name>` for bug fixes.
- Tests and lint must pass locally before you push.
- Fill out every section of the PR template. Placeholder text gets the PR closed without review.
- Describe the problem you solved, not just what you changed.

## What we won't accept

- **Bulk or spray-and-pray PRs.** Don't trawl the issue tracker and open PRs for multiple issues in one session. Pick one, understand it, submit it.
- **Speculative fixes.** "This could theoretically break" isn't a problem statement. If you can't point to a failing test or a reproducible bug, don't submit the PR.
- **Restructuring without evidence.** Changes to core abstractions (the `Backend` ABC, `TollkeeperSession` state machine, `BaseCheck` ABC) need a concrete second use case or a bug report, not a taste preference.
- **New dependencies for what a few lines can do.** This library has zero required dependencies by design. Optional backends (`polars`, `iceberg`) are extras — don't add another.

## Code style

- Ruff handles formatting and linting. Don't hand-format code that `ruff format` would rewrite.
- Python >=3.11 features are fine: `X | None`, `list[T]`, `from __future__ import annotations`.
- Type hints on all functions.
- No comments unless the *why* is non-obvious. Don't narrate what the code already says.

## Testing

- TDD is expected: write the failing test first.
- pytest, with in-memory fakes (e.g. `FakeBackend`) over mocks.
- Coverage must stay at 80% or above.
- Test behavior, not implementation. A test that breaks on a refactor with no behavior change is a bad test.
