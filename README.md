# pyspark-regression
A tool for regression testing Spark Dataframes in Python.

**Note:** This library requires a working intallation of Spark 3.0.3.

Coming Soon:
- Installation via `pip`
- Examples in Databricks Community Notebooks

## What is a Regression Test?
A [Regression Test](https://en.wikipedia.org/wiki/Regression_testing) ensures that changes to code only produce expected outcomes, introducing no _new_ bugs. These tests are particularly challenging when working with database tables, as the result can be too large to visually inspect. When updating a SQL transformation, Data Engineers must ensure that no rows or columns were unintentionally altered, even if the table has 300 columns and 400 billion rows.

`pyspark-regression` reduces the complexity of Regression Testing for structured database tables. It standardizes the concepts and jargon associated with this topic, and implements a clean Python API for running regression tests against DataFrames in [Apache Spark](https://spark.apache.org/) in Python with [PySpark](https://spark.apache.org/docs/latest/api/python/index.html).

## Example
Let's create a simple dataframe called `df_old`.
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    StructType,
    StructField,
)


spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 1)

schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("price", DoubleType()),
    ]
)

df_old = spark.createDataFrame(
    [
        (1, 'Taco', 3.001),
        (2, 'Burrito', 6.50),
        (3, 'flauta', 7.50),
    ],
    schema=schema
)
```

In table form, `df_old` looks like this:
| id | name | price |
| - | - | - |
| 1 | Taco | 3.001 |
| 2 | Burrito | 6.50 |
| 3 | flauta | 7.50 |

There are problems with this data:
1. The price for Tacos needs to be rounded
1. The name for Flautas needs to be capitalized.

Let's create a new dataframe `df_new` with these corrections:
```python
df_new = spark.createDataFrame(
    [
        (1, 'Taco', 3.00),  # Corrected price
        (2, 'Burrito', 6.50),
        (3, 'Flauta', 7.50),  # Corrected name
    ],
    schema=schema
)
```

In table form, `df_new` looks like this:
| id | name | price |
| - | - | - |
| 1 | Taco | **3.00** |
| 2 | Burrito | 6.50 |
| 3 | **Flauta** | 7.50 |

You can use a Regression Test to verify that the changes we made are the changes we expect:
```python
from pyspark_regression.regression import RegressionTest

rt = RegressionTest(
    df_old=df_old,
    df_new=df_new,
    pk='id',
)
```
The `rt` object can now be used to surgically inspect all aspects of how the data has changed. For a general report, you can use the `report` property.

```python
print(rt.report)
```

`report` returns a comprehensive Regression Test analysis in Markdown:
```
# Regression Test: df
- run_id: de9bd4eb-5313-4057-badc-7322ee23b83b
- run_time: 2022-05-25 08:53:50.581283

## Test Results: **FAILURE**.
Printing Regression Report...

### Table stats
- Count records in old df: 3
- Count records in new df: 3
- Count pks in old df: 3
- Count pks in new df: 3

### Diffs
- Columns with diffs: {'name', 'price'}
- Number of records with diffs: 2 (%oT: 66.7%)

 Diff Summary:
| column_name   | data_type   | diff_category            |   count_record | count_record_%oT   |
|:--------------|:------------|:-------------------------|---------------:|:-------------------|
| name          | string      | decapitalization removed |              1 | 33.3%              |
| price         | double      | rounding                 |              1 | 33.3%              |

 Diff Samples: (5 samples per column_name, per diff_category, per is_duplicate)
| column_name   | data_type   |   pk | old_value   | new_value   | diff_category            |
|:--------------|:------------|-----:|:------------|:------------|:-------------------------|
| name          | string      |    3 | 'flauta'    | 'Flauta'    | decapitalization removed |
| price         | double      |    1 | 3.001       | 3.0         | rounding                 |
```

However, all of this information can be accessed directly from properties of the class:
```python
>>> print(rt.count_record_old) # count of records in df_old
3

>>> print(rt.count_record_new) # count of records in df_new
3

>>> print(rt.columns_diff) # Columns with diffs
{'name', 'price'}

>>> rt.df_diff.filter("column_name = 'price'").show() # Show all diffs for 'price' column
+-----------+---------+---+---------+---------+-------------+
|column_name|data_type| pk|old_value|new_value|diff_category|
+-----------+---------+---+---------+---------+-------------+
|      price|   double|  1|    3.001|      3.0|     rounding|
+-----------+---------+---+---------+---------+-------------+
```
