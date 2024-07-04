# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
## [0.9.21] - 2024-07-4
- Upgrade qrlew

## [0.9.20] - 2024-05-29
### Added
- Add wrapper for RelationWithDPEvent
- type() method for Relation

### Fixed
- Dataset with admin cols from engine only if tables have admin cols

## [0.9.19] - 2024-05-29
### Added
- Add strategy argument to privacy_unit_preserving [MR54](https://github.com/Qrlew/pyqrlew/pull/54)
- Add with_field, rename_fields and compose methods to Relation. [MR54](https://github.com/Qrlew/pyqrlew/pull/54)

## [0.9.14] - 2024-03-19
### Fixed
- example notebook rewrite_with_dp
### Added
- mypy checking in the CI [MR45](https://github.com/Qrlew/pyqrlew/pull/45)
### Changed
- improving the doc [MR44](https://github.com/Qrlew/pyqrlew/pull/44)

## [0.9.11] - 2024-01-30
- Updated version (unique + dedup relation names)

## [0.9.10] - 2024-01-29
- Support manual multiplicity clipping
- Added more explicit error

## [0.9.7] - 2024-01-29
### Added
- support for bigquery dialect [MR42](https://github.com/Qrlew/pyqrlew/pull/42)
### Fixed
- quoting of query identifiers [MR42](https://github.com/Qrlew/pyqrlew/pull/42)
### Changed
- law for clipping bounds when DP rewriting [MR42](https://github.com/Qrlew/pyqrlew/pull/42)

## [0.9.6] - 2024-01-29
### Fixed
Fixing the example notebooks [MR40](https://github.com/Qrlew/pyqrlew/pull/40)
### Added
Adding an example notebook with MsSqlTranslator [MR40](https://github.com/Qrlew/pyqrlew/pull/40)

## [0.9.5] - 2024-01-17
### Added
- Simpler access to Relation
- Translator features with MSSQL support.
- Dialect Enum.

### Changed
- Dataset sql(&self, query: &str) method changed to relation(&self, query: &str, dialect: Option<Dialect>).
- Added optional dialect in the signature of Dataset's from_queries method.
- Relation render() method changed to to_query(dialect: Option<Dialect>).
- Relation parse(query: &str, dataset: &Dataset) method changed to from_query(query: &str, dataset: &Dataset, dialect: Option<Dialect>).

## [0.8.2] - 2024-01-04
### Changed
- Update versions
- Change Dataset API to set bounds and constraints

## [0.7.3] - 2024-01-04
### Removed
- Dependency to matplotlib removed

## [0.7.1] - 2023-12-28
### Changed
- Update qrlew version

## [0.7.0] - 2023-12-22
### Changed
- Update qrlew and make synthetic dato optional for rewritting into DP [MR27](https://github.com/Qrlew/pyqrlew/pull/27)
### Added
- Stochastic tester [MR22](https://github.com/Qrlew/pyqrlew/pull/22)
### Fixed
- Query for fetching the possible values when loading the dataset  [MR27](https://github.com/Qrlew/pyqrlew/pull/27)


## [0.4.5] - 2023-10-29
### Changed
- Use the last version of qrlew

## [0.4.4] - 2023-10-29
### Changed
- Use the last version of qrlew

## [0.4.2] - 2023-10-28
### Changed
- Updated Qrlew version
- Added PEP compilation

## [0.4.1] - 2023-10-26
### Fixed
- Fix synthetic_data argument type for Relation's rewrite_with_differential_privacy [MR20](https://github.com/Qrlew/pyqrlew/pull/20)

## [0.4.0] - 2023-10-26
### Changed
- Updated qrlew version and dp compilation

## [0.3.8] - 2023-09-29
### Added
- Dataset from queries [MR19](https://github.com/Qrlew/pyqrlew/pull/19)

## [0.3.7] - 2023-09-29
### Added
- `PrivateQuery` [MR17](https://github.com/Qrlew/pyqrlew/pull/17)

## [0.3.6] - 2023-09-29
### Changed
- tau_thresholding epsilon and delta can be set

## [0.3.5] - 2023-09-29
### Changed
- Updated qrlew where objects are threadsafe now

## [0.3.4] - 2023-09-28
### Changed
- update qrlew version to 0.3 [MR15](https://github.com/Qrlew/pyqrlew/pull/15)
- downgrade sqlalchemy to 1.4.* [MR15](https://github.com/Qrlew/pyqrlew/pull/15)
- downgrade to psycopg2 [MR15](https://github.com/Qrlew/pyqrlew/pull/15)

## [0.3.3] - 2023-08-29
### Changed
- split lib.rs into 3 files[MR14](https://github.com/Qrlew/pyqrlew/pull/14)
- Updated qrlew dep
### Fixed
- Visitor for protobuf files (set the size)[MR14](https://github.com/Qrlew/pyqrlew/pull/14)

## [0.3.2] - 2023-08-03
### Added
- support for SQLite
- retail Dataset + notebook `range_propagation.ipynb`

## [0.2.1] - 2023-07-18
### Changed
- Updated `qrlew-datasets`

## [0.2.0] - 2023-07-18
### Changed
- Remove data and db from pyqrlew. They are now in the qrlew-dataset package.
- The example notebook has been updated.

## [0.1.0] - 2023-07-11

### Added

- Changelog file

### Fixed

- Automated Postgresql setup if not already the case

### Changed

- Upgrade dependencies: Ruby 3.2.1, Middleman, etc.

### Removed

- Cleaned the test environment
