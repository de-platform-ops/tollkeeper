# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project scaffolding.
- Tollkeeper core library logic with state machine lifecycle (`TollkeeperSession`).
- `pyiceberg` and `polars` optional dependency integrations.
- Local CSV and PyIceberg backends for branching, writing, and rolling back.
- Basic Polars dataframe validation checks.

### Changed
- Refactored checks to utilize lazy evaluation (`scan_csv`) over eager loading for performance.
- Moved `__del__` rollback logic to formal Python Context Manager (`__enter__` / `__exit__`).

## [0.1.0] - TBD

- _First official release to PyPI._
