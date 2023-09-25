# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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
