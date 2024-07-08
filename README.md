# pyspark-regression

`pyspark-regression` is a concise, no-nonsense library for regression testing between PySpark Dataframes.

For install instructions and API documentation, please visit https://forrest-bajbek.github.io/pyspark-regression/


## What is a Regression Test?
A [Regression Test](https://en.wikipedia.org/wiki/Regression_testing) ensures that changes to code only produce expected outcomes, introducing no _new_ bugs. These tests are particularly challenging when working with database tables, as the result can be too large to visually inspect. When updating a SQL transformation, Data Engineers must ensure that no rows or columns were unintentionally altered, even if the table has hundreds columns and billions of rows.

`pyspark-regression` reduces the complexity of Regression Testing by implementing a clean Python API for running regression tests between DataFrames in [Apache Spark](https://spark.apache.org/).

## Example
Consider the following table:

| id | name | price |
| - | - | - |
| 1 | Taco | 3.001 |
| 2 | Burrito | 6.50 |
| 3 | flauta | 7.50 |

Imagine you are a Data Engineer, and you want to change the underlying ETL so that:

1. The price for Tacos is rounded to 2 decimal places.
1. The name for Flautas is capitalized.

You make your changes, and the new table looks like this:

| id | name | price |
| - | - | - |
| 1 | Taco | **3.00** |
| 2 | Burrito | 6.50 |
| 3 | **Flauta** | 7.50 |

Running a regression test will help you confirm that the new ETL changed the data how you expected.

Let's create the old and new tables as dataframes so we can run a Regression Test:
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark_regression import RegressionTest

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 1)

schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("price", DoubleType()),
    ]
)

# The old data
df_old = spark.createDataFrame(
    [
        (1, 'Taco', 3.001),
        (2, 'Burrito', 6.50),
        (3, 'flauta', 7.50),
    ],
    schema=schema
)

# The new data
df_new = spark.createDataFrame(
    [
        (1, 'Taco', 3.00),  # Corrected price
        (2, 'Burrito', 6.50),
        (3, 'Flauta', 7.50),  # Corrected name
    ],
    schema=schema
)

regression_test = RegressionTest(
    df_old=df_old,
    df_new=df_new,
    pk='id',
)
```


`RegressionTest()` returns a Python class with properties that let you inspect the differences between dataframes. Most notably, the `summary` property prints a comprehensive analysis in Markdown.
```markdown
>>> print(regression_test.summary)

# Regression Test: df
- run_id: de9bd4eb-5313-4057-badc-7322ee23b83b
- run_time: 2022-05-25 08:53:50.581283

## Result: **FAILURE**.
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
| column_name   | data_type   | diff_category        |   count_record | count_record_%oT   |
|:--------------|:------------|:---------------------|---------------:|:-------------------|
| name          | string      | capitalization added |              1 | 33.3%              |
| price         | double      | rounding             |              1 | 33.3%              |

 Diff Samples: (5 samples per column_name, per diff_category, per is_duplicate)
| column_name   | data_type   |   pk | old_value   | new_value   | diff_category        |
|:--------------|:------------|-----:|:------------|:------------|:---------------------|
| name          | string      |    3 | 'flauta'    | 'Flauta'    | capitalization added |
| price         | double      |    1 | 3.001       | 3.0         | rounding             |
```

The `RegressionTest` class provides low level access to all the methods used to build the summary:
```python
>>> print(regression_test.count_record_old) # count of records in df_old
3

>>> print(regression_test.count_record_new) # count of records in df_new
3

>>> print(regression_test.columns_diff) # Columns with diffs
{'name', 'price'}

>>> regression_test.df_diff.filter("column_name = 'price'").show() # Show all diffs for 'price' column
+-----------+---------+---+---------+---------+-------------+
|column_name|data_type| pk|old_value|new_value|diff_category|
+-----------+---------+---+---------+---------+-------------+
|      price|   double|  1|    3.001|      3.0|     rounding|
+-----------+---------+---+---------+---------+-------------+
```