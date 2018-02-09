## 0.2.2 [unreleased]

- Added support for MySQL replication lag
- Added `replica` option

## 0.2.1

- Fixed lag check for Postgres 10
- Added `replication_lag` method

## 0.2.0

Breaking

- Jobs default to replica when `default_to_primary` is false

Other

- Replaced `default_to_primary` with `by_default`
- Fixed `max_lag` option
- Added `lag_failover` option
- Added `failover` option
- Added `lag_on` option
- Added `primary` option
- Added default options
- Improved lag query

## 0.1.2

- Raise `ArgumentError` when missing block
- Improved lag query
- Warn if returning `ActiveRecord::Relation`

## 0.1.1

- Added method for jobs

## 0.1.0

- First release
