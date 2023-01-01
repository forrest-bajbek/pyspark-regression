# Home

## `pyspark-regression`
`pyspark-regression` is a tool for regression testing Spark Dataframes in Python.

## What is a Regression Test?
A [Regression Test](https://en.wikipedia.org/wiki/Regression_testing) ensures that changes to code only produce expected outcomes, introducing no _new_ bugs. These tests are particularly challenging when working with database tables, as the result can be too large to visually inspect. When updating a SQL transformation, Data Engineers must ensure that no rows or columns were unintentionally altered, even if the table has 300 columns and 400 billion rows.

`pyspark-regression` reduces the complexity of Regression Testing for structured database tables. It standardizes the concepts and jargon associated with this topic, and implements a clean Python API for running regression tests against DataFrames in [Apache Spark](https://spark.apache.org/).

