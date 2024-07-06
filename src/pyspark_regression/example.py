from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pyspark_regression.regression import RegressionTest

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
        (1, "Taco", 3.001),
        (2, "Burrito", 6.50),
        (3, "flauta", 7.50),
    ],
    schema=schema,
)

# The new data
df_new = spark.createDataFrame(
    [
        (1, "Taco", 3.00),  # Corrected price
        (2, "Burrito", 6.50),
        (3, "Flauta", 7.50),  # Corrected name
    ],
    schema=schema,
)

regression_test = RegressionTest(
    df_old=df_old,
    df_new=df_new,
    pk="id",
)


print(regression_test.summary)