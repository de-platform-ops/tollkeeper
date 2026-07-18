# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Documentation site with GitHub Pages deploy (#23).
- Guides for the signal store, data quality checks, and the SQL lineage parser (#27).

### Changed
- Renamed the GitHub org from srchilukoori to de-platform-ops, updated the logo, and updated project contact email to sage.quotient@gmail.com (#25, #26).
- Tightened security lint rules and cleaned up leftover resources from the beta release (#25).

### Fixed
- Bumped GitHub Actions dependencies via the dependabot actions-dependencies group (#28).

## [0.1.0b1] - First PyPI beta release

### Added
- Initial WAP (write-audit-publish) core library with fluent API, CI pipeline, and README (#1).
- `CsvBackend` and Polars-based data quality checks (#2).
- Signal store for cross-pipeline audit coordination (#3).
- `IcebergBackend` for branching, writing, and rolling back Iceberg tables (#5).
- `airflow-wap` package with `WAPOperator`, `WAPSensor`, and an engine resolver (#8).
- Standard OSS repo templates and configuration: PR template, issue forms, contribution guide, security policy, dependabot config (#7, #10, #14).
- SQL AST parser built on sqlglot for automatic lineage extraction (#16).
- WAP `TaskGroup` factory for automatic sensor wiring (#18).
- SQL passthrough strategy, data quality operators, and example DAGs (#21).
- uv workspace layout and version bump for the 0.1.0b1 beta release (#22).

### Changed
- Renamed the project from write-audit-publish to Tollkeeper, including all WAP references across code and docs (#19, #20).
- Rewrote the README to meet OSS project standards (#17).

### Fixed
- Critical session lifecycle bugs in `WAPSession` (#4).
- Review bugs covering the context manager, lazy CSV scan, atomic rename, staging garbage collection, and optional dependency handling (#6).
- Moderate bugs in the Iceberg backend, plus completed documentation (#5).
- Dependabot dependency bumps for GitHub Actions (#15).
