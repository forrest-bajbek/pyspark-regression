# pyspark-regression
A tool for regression testing Spark Dataframes in Python.

## What is a Regression Test?
A [Regression Test](https://en.wikipedia.org/wiki/Regression_testing) ensures that changes to code only produce expected outcomes, introducing no _new_ bugs. These tests are particularly challenging when working with database tables, as the result can be too large to visually inspect. When updating a SQL transformation, Data Engineers must ensure that no rows or columns were unintentionally altered, even if the table has 300 columns and 400 billion rows.

`pyspark-regression` reduces the complexity of Regression Testing for structured database tables. It standardizes the concepts and jargon associated with this topic, and implements a clean Python API for running regression tests against DataFrames in [Apache Spark](https://spark.apache.org/) in Python with [PySpark](https://spark.apache.org/docs/latest/api/python/index.html).

## Concepts

### General Terms
pk

### Column Change

#### New
#### Old
#### All
#### Added
#### Removed
#### Kept

### Schema Mutation

#### Type
#### Nullable
#### Metadata

### Duplicate

- Duplicates in the old table (that are not in the new table) indicate a problem has been fixed!
- Duplicates in the new table (that are not in the old table) indicate the introduction of a one-to-many join condition.
- If the exact same duplicates are in both tables, `RegressionTest.has_symmetric_duplicates == True`

### Orphan
Primary Keys are considered "orphaned" when they are in one table but not the other. When a `pk` is in `table_old` but _not_ `table_new`, `pk` is an orphan in `table_old`.

- Orphans in the old table indicate more restrictive filter or inner join requirements.
- Orphans in the new table indicate less restrictive filter or inner join requirements.

### Diff

#### Diff Summary
