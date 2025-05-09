from datetime import date, datetime

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DateType, DoubleType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType
from tabulate import tabulate  # type: ignore [import-untyped]

from pyspark_regression import RegressionTest, SchemaMutation


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.config("spark.sql.shuffle.partitions", "1").config("spark.sql.analyzer.failAmbiguousSelfJoin", False).getOrCreate()


@pytest.fixture(scope="session")
def simple_schema():
    return StructType(
        [
            StructField("id", StringType()),
            StructField("attr_1", StringType()),
            StructField("attr_2", StringType()),
        ]
    )


@pytest.fixture(scope="session")
def empty_df(spark, simple_schema):
    return spark.createDataFrame([], schema=simple_schema)


@pytest.fixture(scope="session")
def simple_df(spark, simple_schema):
    return spark.createDataFrame(
        [
            (1, "a", "!"),
            (2, "b", "@"),
            (3, "c", "#"),
        ],
        schema=simple_schema,
    )


def test_post_init_pk_exists(spark, empty_df):
    """
    If pk doesn't exist in either df_old or df_new, an error should be raised.
    """
    df_old = empty_df
    df_new = empty_df
    with pytest.raises(KeyError):
        RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="wrong_key")


def test_post_init_pk_reserved(spark, empty_df):
    """
    If 'pk' itself is passed for pk, an error should be raised.
    """
    df_old = empty_df
    df_new = empty_df
    with pytest.raises(KeyError):
        RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="pk")


def test_columns_old(spark, empty_df):
    rt = RegressionTest(spark=spark, df_old=empty_df, df_new=empty_df, pk="id")
    assert rt.columns_old == set(empty_df.columns)


def test_columns_new(spark, empty_df):
    rt = RegressionTest(spark=spark, df_old=empty_df, df_new=empty_df, pk="id")
    assert rt.columns_new == set(empty_df.columns)


def test_columns_all(spark, empty_df):
    df_old = empty_df.withColumn("attr_3", F.lit(None))
    df_new = empty_df.withColumn("attr_4", F.lit(None))
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.columns_all == set(empty_df.columns + ["attr_3", "attr_4"])


def test_columns_added(spark, empty_df):
    df_old = empty_df
    df_new = empty_df.withColumn("attr_3", F.lit(None))
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.columns_added == set(["attr_3"])


def test_columns_removed(spark, empty_df):
    df_old = empty_df.withColumn("attr_3", F.lit(None))
    df_new = empty_df
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.columns_removed == set(["attr_3"])


def test_columns_kept(spark, empty_df):
    rt = RegressionTest(spark=spark, df_old=empty_df, df_new=empty_df, pk="id")
    assert rt.columns_kept == set(empty_df.columns)


def test_schema_mutations_type(spark, empty_df):
    df_old = empty_df.withColumn("attr_3", F.lit(None).cast(DateType()))
    df_new = empty_df.withColumn("attr_3", F.lit(None).cast(TimestampType()))
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.schema_mutations_type == set(
        [
            SchemaMutation(
                column_name="attr_3",
                attribute="type",
                value_old="date",
                value_new="timestamp",
            )
        ]
    )


def test_columns_changed_type(spark, empty_df):
    df_old = empty_df.withColumn("attr_3", F.lit(None).cast(DateType()))
    df_new = empty_df.withColumn("attr_3", F.lit(None).cast(TimestampType()))
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.columns_changed_type == set(["attr_3"])


def test_columns_comparable(spark, empty_df):
    df_old = empty_df.withColumn("attr_3", F.lit(None).cast(DateType()))
    df_new = empty_df.withColumn("attr_3", F.lit(None).cast(TimestampType()))
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.columns_comparable == set(empty_df.columns)


def test_count_record_old(spark, simple_df):
    rt = RegressionTest(spark=spark, df_old=simple_df, df_new=simple_df, pk="id")
    assert rt.count_record_old == simple_df.count()


def test_count_record_new(spark, simple_df):
    rt = RegressionTest(spark=spark, df_old=simple_df, df_new=simple_df, pk="id")
    assert rt.count_record_new == simple_df.count()


def test_count_pk_old(spark, simple_df):
    rt = RegressionTest(spark=spark, df_old=simple_df, df_new=simple_df, pk="id")
    assert rt.count_pk_old == simple_df.select(simple_df.id).distinct().count()


def test_count_pk_new(spark, simple_df):
    rt = RegressionTest(spark=spark, df_old=simple_df, df_new=simple_df, pk="id")
    assert rt.count_pk_new == simple_df.select(simple_df.id).distinct().count()


def test_df_duplicate_old(spark, simple_df):
    df_old = simple_df.unionAll(simple_df)
    df_new = simple_df
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    df_duplicate = (
        df_old.select(F.col("id").alias("pk"))
        .groupBy(F.col("pk"))
        .agg((F.count(F.col("pk")) - 1).alias("count_duplicate_record"))
        .filter(F.col("count_duplicate_record") > 0)
    )
    assert rt.df_duplicate_old.exceptAll(df_duplicate).count() == 0


def test_df_duplicate_old_2(spark, simple_df):
    df_old = simple_df
    df_new = simple_df.unionAll(simple_df)
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    df_duplicate = (
        df_new.select(F.col("id").alias("pk"))
        .groupBy(F.col("pk"))
        .agg((F.count(F.col("pk")) - 1).alias("count_duplicate_record"))
        .filter(F.col("count_duplicate_record") > 0)
    )
    assert rt.df_duplicate_old.exceptAll(df_duplicate).count() == 0


def test_count_record_duplicate_old(spark, simple_df):
    df_old = simple_df.unionAll(simple_df)
    df_new = simple_df
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.count_record_duplicate_old == df_old.count() - df_old.select(F.col("id")).distinct().count()


def test_count_record_duplicate_old_2(spark, simple_df):
    df_old = simple_df
    df_new = simple_df.unionAll(simple_df)
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.count_record_duplicate_new == df_new.count() - df_new.select(F.col("id")).distinct().count()


def test_count_pk_duplicate_old(spark, simple_df):
    df_old = simple_df.unionAll(simple_df)
    df_new = simple_df
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.count_record_duplicate_old == df_old.select(F.col("id")).distinct().count()


def test_count_pk_duplicate_old_2(spark, simple_df):
    df_old = simple_df
    df_new = simple_df.unionAll(simple_df)
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.count_record_duplicate_new == df_new.select(F.col("id")).distinct().count()


def test_sample_pk_duplicate_old(spark, simple_df):
    df_old = simple_df.unionAll(simple_df)
    df_new = simple_df
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    df_result = df_old.select(F.col("id")).distinct().orderBy(F.col("id"))
    assert rt.sample_pk_duplicate_old == tuple([row.id for row in df_result.collect()])


def test_sample_pk_duplicate_new(spark, simple_df):
    df_old = simple_df
    df_new = simple_df.unionAll(simple_df)
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    df_result = df_new.select(F.col("id")).distinct().orderBy(F.col("id"))
    assert rt.sample_pk_duplicate_new == tuple([row.id for row in df_result.collect()])


def test_has_symmetric_duplicates_true(spark, simple_df):
    df_old = simple_df.unionAll(simple_df)
    df_new = simple_df.unionAll(simple_df)
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.has_symmetric_duplicates is True


def test_has_symmetric_duplicates_false(spark, simple_df):
    df_old = simple_df.unionAll(simple_df)
    df_new = simple_df
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    assert rt.has_symmetric_duplicates is False


def test_df_orphan_old(spark, simple_df):
    df_old = simple_df.unionAll(simple_df)
    df_new = simple_df
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    df_orphan = df_old.join(df_new, how="left_anti", on=["id"]).select(F.col("id")).distinct()
    assert rt.df_orphan_old.exceptAll(df_orphan).count() == 0


def test_df_orphan_new(spark, simple_df):
    df_old = simple_df
    df_new = simple_df.unionAll(simple_df)
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    df_orphan = df_new.join(df_old, how="left_anti", on=["id"]).select(F.col("id")).distinct()
    assert rt.df_orphan_old.exceptAll(df_orphan).count() == 0


def test_count_pk_orphan_old(spark, simple_df):
    df_old = simple_df.unionAll(simple_df)
    df_new = simple_df
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    count_orphan_pk = df_old.join(df_new, how="left_anti", on=["id"]).select(F.col("id")).distinct().count()
    assert rt.count_pk_orphan_old == count_orphan_pk


def test_count_pk_orphan_new(spark, simple_df):
    df_old = simple_df
    df_new = simple_df.unionAll(simple_df)
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    count_orphan_pk = df_new.join(df_old, how="left_anti", on=["id"]).select(F.col("id")).distinct().count()
    assert rt.count_pk_orphan_new == count_orphan_pk


def test_sample_pk_orphan_old(spark, simple_df):
    df_old = simple_df.unionAll(simple_df)
    df_new = simple_df
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    df_result = df_old.join(df_new, how="left_anti", on=["id"]).select(F.col("id")).distinct()
    sample_orphan_pk = tuple([row.id for row in df_result.collect()])
    assert rt.sample_pk_orphan_old == sample_orphan_pk


def test_sample_pk_orphan_new(spark, simple_df):
    df_old = simple_df
    df_new = simple_df.unionAll(simple_df)
    rt = RegressionTest(spark=spark, df_old=df_old, df_new=df_new, pk="id")
    df_result = df_new.join(df_old, how="left_anti", on=["id"]).select(F.col("id")).distinct()
    sample_orphan_pk = tuple([row.id for row in df_result.collect()])
    assert rt.sample_pk_orphan_new == sample_orphan_pk


def test_count_pk_comparable(spark, simple_df):
    rt = RegressionTest(spark=spark, df_old=simple_df, df_new=simple_df, pk="id")
    assert rt.count_pk_comparable == simple_df.select(F.col("id")).distinct().count()


def test_diff(spark):
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("attr_str", StringType()),
            StructField("attr_bool", BooleanType()),
            StructField("attr_int", IntegerType()),
            StructField("attr_double", DoubleType()),
            StructField("attr_float", FloatType()),
            StructField("attr_date", DateType()),
            StructField("attr_timestamp", TimestampType()),
        ]
    )

    df_old = spark.createDataFrame(
        [
            (1, None, True, None, None, None, None, None),  # NULL -> NULL (same)
            (2, "a", True, 1, 1.0, 1.00000, date(2022, 1, 1), datetime(2022, 1, 1, 12, 0, 0, 0)),  # NOT NULL -> NOT NULL (same)
            (3, "a", True, 1, 1.0, 1.00000, date(2022, 1, 1), datetime(2022, 1, 1, 12, 0, 0, 0)),  # NOT NULL -> NOT NULL (diff)
            (4, None, True, None, None, None, None, None),  # NULL -> NOT NULL
            (5, "a", True, 1, 1.0, 1.00000, date(2022, 1, 1), datetime(2022, 1, 1, 12, 0, 0, 0)),  # NOT NULL -> NULL
            (6, "  pad removed  ", None, None, None, None, None, None),  # pad removed
            (7, "pad added", None, None, None, None, None, None),  # pad added
            (8, "  lpad removed", None, None, None, None, None, None),  # lpad removed
            (9, "lpad added", None, None, None, None, None, None),  # lpad added
            (10, "  rpad removed", None, None, None, None, None, None),  # rpad removed
            (11, "rpad added", None, None, None, None, None, None),  # rpad added
            (12, "CAP REMOVED", None, None, None, None, None, None),  # cap removed
            (13, "Cap Added", None, None, None, None, None, None),  # cap added
            (14, "decap removed", None, None, None, None, None, None),  # decap removed
            (15, "DeCap Added", None, None, None, None, None, None),  # decap added
            (16, "Cap changed", None, None, None, None, None, None),  # cap changed
            (17, "truncation added", None, None, None, None, None, None),  # truncated
            (18, "trunca", None, None, None, None, None, None),  # extended
            (19, None, None, None, None, 1.0000, None, None),  # rounding
            (20, None, None, None, None, 1.0001, None, None),  # rounding
            (21, None, None, None, None, None, None, datetime(2022, 1, 1, 12, 0, 0, 0)),  # hour shift
            (22, None, None, None, None, None, None, datetime(2022, 1, 1, 15, 0, 0, 0)),  # hour shift
        ],
        schema=schema,
    )

    df_new = spark.createDataFrame(
        [
            (1, None, True, None, None, None, None, None),  # NULL -> NULL (same)
            (2, "a", True, 1, 1.0, 1.00000, date(2022, 1, 1), datetime(2022, 1, 1, 12, 0, 0, 0)),  # NOT NULL -> NOT NULL (same)
            (3, "b", False, 2, 2.0, 2.00000, date(2022, 1, 2), datetime(2022, 1, 2, 12, 0, 0, 0)),  # NOT NULL -> NOT NULL (diff), boolean flip
            (4, "a", True, 1, 1.0, 1.00000, date(2022, 1, 1), datetime(2022, 1, 1, 12, 0, 0, 0)),  # NULL -> NOT NULL
            (5, None, True, None, None, None, None, None),  # NOT NULL -> NULL
            (6, "pad removed", None, None, None, None, None, None),  # pad removed
            (7, "  pad added  ", None, None, None, None, None, None),  # pad added
            (8, "lpad removed", None, None, None, None, None, None),  # lpad removed
            (9, "  lpad added", None, None, None, None, None, None),  # lpad added
            (10, "rpad removed", None, None, None, None, None, None),  # rpad removed
            (11, "rpad added  ", None, None, None, None, None, None),  # rpad added
            (12, "Cap Removed", None, None, None, None, None, None),  # cap removed
            (13, "CAP ADDED", None, None, None, None, None, None),  # cap added
            (14, "DeCap Removed", None, None, None, None, None, None),  # decap removed
            (15, "decap added", None, None, None, None, None, None),  # decap added
            (16, "cAP CHANGED", None, None, None, None, None, None),  # cap changed
            (17, "trunca", None, None, None, None, None, None),  # truncated
            (18, "truncation removed", None, None, None, None, None, None),  # extended
            (19, None, None, None, None, 1.0001, None, None),  # rounding
            (20, None, None, None, None, 1.0000, None, None),  # rounding
            (21, None, None, None, None, None, None, datetime(2022, 1, 1, 15, 0, 0, 0)),  # hour shift
            (22, None, None, None, None, None, None, datetime(2022, 1, 1, 12, 0, 0, 0)),  # hour shift
        ],
        schema=schema,
    )

    rt = RegressionTest(
        spark=spark,
        df_old=df_old,
        df_new=df_new,
        pk="id",
        table_name="test_diff",
    )

    assert rt.count_pk_comparable == 22
    assert rt.count_record_diff == 20
    assert rt.count_pk_diff == 20
    assert not rt.success
    assert (
        tabulate(
            rt.df_diff.orderBy(F.col("pk").cast(IntegerType()), F.col("diff_category"), F.col("column_name")).toPandas(),
            headers="keys",
            missingval="NULL",
            tablefmt="pipe",
            showindex=False,
        )
        == """\
| column_name    | data_type   |   pk | old_value           | new_value            | diff_category                    |
|:---------------|:------------|-----:|:--------------------|:---------------------|:---------------------------------|
| attr_bool      | boolean     |    3 | true                | false                | boolean flip (true -> false)     |
| attr_timestamp | timestamp   |    3 | 2022-01-01 12:00:00 | 2022-01-02 12:00:00  | hour shift                       |
| attr_double    | double      |    3 | 1.0                 | 2.0                  | multiple                         |
| attr_float     | float       |    3 | 1.0                 | 2.0                  | multiple                         |
| attr_date      | date        |    3 | 2022-01-01          | 2022-01-02           | uncategorized                    |
| attr_int       | integer     |    3 | 1                   | 2                    | uncategorized                    |
| attr_str       | string      |    3 | 'a'                 | 'b'                  | uncategorized                    |
| attr_date      | date        |    4 | NULL                | 2022-01-01           | null flip (null -> not null)     |
| attr_double    | double      |    4 | NULL                | 1.0                  | null flip (null -> not null)     |
| attr_float     | float       |    4 | NULL                | 1.0                  | null flip (null -> not null)     |
| attr_int       | integer     |    4 | NULL                | 1                    | null flip (null -> not null)     |
| attr_str       | string      |    4 | NULL                | 'a'                  | null flip (null -> not null)     |
| attr_timestamp | timestamp   |    4 | NULL                | 2022-01-01 12:00:00  | null flip (null -> not null)     |
| attr_date      | date        |    5 | 2022-01-01          | NULL                 | null flip (not null -> null)     |
| attr_double    | double      |    5 | 1.0                 | NULL                 | null flip (not null -> null)     |
| attr_float     | float       |    5 | 1.0                 | NULL                 | null flip (not null -> null)     |
| attr_int       | integer     |    5 | 1                   | NULL                 | null flip (not null -> null)     |
| attr_str       | string      |    5 | 'a'                 | NULL                 | null flip (not null -> null)     |
| attr_timestamp | timestamp   |    5 | 2022-01-01 12:00:00 | NULL                 | null flip (not null -> null)     |
| attr_str       | string      |    6 | '  pad removed  '   | 'pad removed'        | padding removed (left and right) |
| attr_str       | string      |    7 | 'pad added'         | '  pad added  '      | padding added (left and right)   |
| attr_str       | string      |    8 | '  lpad removed'    | 'lpad removed'       | padding removed (left)           |
| attr_str       | string      |    9 | 'lpad added'        | '  lpad added'       | padding added (left)             |
| attr_str       | string      |   10 | '  rpad removed'    | 'rpad removed'       | padding removed (left)           |
| attr_str       | string      |   11 | 'rpad added'        | 'rpad added  '       | padding added (right)            |
| attr_str       | string      |   12 | 'CAP REMOVED'       | 'Cap Removed'        | capitalization removed           |
| attr_str       | string      |   13 | 'Cap Added'         | 'CAP ADDED'          | capitalization added             |
| attr_str       | string      |   14 | 'decap removed'     | 'DeCap Removed'      | capitalization added             |
| attr_str       | string      |   15 | 'DeCap Added'       | 'decap added'        | capitalization removed           |
| attr_str       | string      |   16 | 'Cap changed'       | 'cAP CHANGED'        | capitalization changed           |
| attr_str       | string      |   17 | 'truncation added'  | 'trunca'             | truncation added                 |
| attr_str       | string      |   18 | 'trunca'            | 'truncation removed' | truncation removed               |
| attr_float     | float       |   19 | 1.0                 | 1.0001               | rounding                         |
| attr_float     | float       |   20 | 1.0001              | 1.0                  | rounding                         |
| attr_timestamp | timestamp   |   21 | 2022-01-01 12:00:00 | 2022-01-01 15:00:00  | hour shift                       |
| attr_timestamp | timestamp   |   22 | 2022-01-01 15:00:00 | 2022-01-01 12:00:00  | hour shift                       |"""
    )
