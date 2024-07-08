import functools
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import LongType, StringType
from tabulate import tabulate

spark = SparkSession.builder.getOrCreate()


@dataclass(frozen=True)
class SchemaMutation:
    column_name: str
    "Name of the column"
    attribute: str
    "Column attribute defined in DataFrame ([DDL](https://en.wikipedia.org/wiki/Data_definition_language))"
    value_old: str
    "Value in df_old"
    value_new: str
    "Value in df_new"


class RegressionTest:
    def __init__(
        self,
        df_old: DataFrame,
        df_new: DataFrame,
        pk: str | Column,
        *,
        table_name: str = "df",
        num_samples: int = 5,
        sort_samples: bool = False,
    ):
        """A class with methods for regression testing between two DataFrames.

        Args:
            df_old (DataFrame): Old Spark DataFrame
            df_new (DataFrame): New Spark DataFrame
            pk (str): Primary key shared between df_old and df_new, can be either a string or a Spark Column
            table_name (str, optional): Name of table. Defaults to "df".
            num_samples (int, optional): Number of samples to return in sample fields. Defaults to 5.
            sort_samples (bool, optional): If True, will sort DataFrames before retrieving samples. Ensures that samples are idempotent, but reduces performance. Defaults to False.

        Raises:
            KeyError: Raises if pk is missing from `df_old` or `df_new`, or if pk = 'pk'.
            e: Any error raised when calculating if duplicates are symmetrical.

        Returns:
            RegressionTestResult: Dataclass containing regression test results
        """

        if pk not in df_old.columns:
            raise KeyError(f"Primary key '{pk}' is missing from df_old.")
        if pk not in df_new.columns:
            raise KeyError(f"Primary key '{pk}' is missing from df_new.")

        if "pk" in df_old.columns:
            raise KeyError("The column name 'pk' itself is reserved. Please remove it from df_old.")
        if "pk" in df_new.columns:
            raise KeyError("The column name 'pk' itself is reserved. Please remove it from df_new.")

        self._df_old = df_old.withColumn("pk", F.col(pk))
        self._df_new = df_new.withColumn("pk", F.col(pk))
        self._pk = pk
        self._table_name = table_name
        self._num_samples = num_samples
        self._sort_samples = sort_samples
        self._run_id = str(uuid4())

        p = Path(f"/tmp/pyspark-regression/{self.run_id}")
        p.parent.mkdir(exist_ok=True)
        spark.sparkContext.setCheckpointDir(p.as_posix())
        self._df_cache: dict[str, DataFrame] = dict()

    # Make attributes read-only
    # ---------------------------------------------------------------------------------
    @property
    def df_old(self) -> DataFrame:
        "Old Spark DataFrame"
        return self._df_old

    @property
    def df_new(self) -> DataFrame:
        "New Spark DataFrame"
        return self._df_new

    @property
    def pk(self) -> str | Column:
        "Primary key shared between df_old and df_new"
        return self._pk

    @property
    def table_name(self) -> str:
        "Name of table"
        return self._table_name

    @property
    def num_samples(self) -> int:
        "Number of samples to return in sample fields"
        return self._num_samples

    @property
    def sort_samples(self) -> bool:
        "If True, will sort DataFrames before retrieving samples. Ensures that samples are idempotent, but reduces performance."
        return self._sort_samples

    @property
    def run_id(self) -> str:
        "The regression test's unique identifier"
        return self._run_id

    # Metadata
    # ---------------------------------------------------------------------------------
    @cached_property
    def columns_old(self) -> set[str]:
        "Columns in `df_old`"
        return set([col for col in self.df_old.columns if not col == "pk"])

    @cached_property
    def columns_new(self) -> set[str]:
        "Columns in `df_new`"
        return set([col for col in self.df_new.columns if not col == "pk"])

    @cached_property
    def columns_all(self) -> set[str]:
        "All columns between `df_old` and `df_new`"
        return self.columns_new.union(self.columns_old)

    @cached_property
    def columns_added(self) -> set[str]:
        "Columns in `df_new` that are not in `df_old`"
        return self.columns_new.difference(self.columns_old)

    @cached_property
    def columns_removed(self) -> set[str]:
        "Columns in `df_old` that are not in `df_new`"
        return self.columns_old.difference(self.columns_new)

    @cached_property
    def columns_kept(self) -> set[str]:
        "Columns in both `df_old` and `df_new`"
        return self.columns_old.intersection(self.columns_new)

    @cached_property
    def schema_mutations(self) -> set[SchemaMutation]:
        "Detail for all schema mutations between `df_old` and `df_new`"
        schema_mutations = set()
        for col in self.columns_kept:
            schema_old = self.df_old.schema[col].jsonValue()
            schema_new = self.df_new.schema[col].jsonValue()
            for attribute in schema_old.keys():
                if schema_old[attribute] != schema_new[attribute]:
                    schema_mutations.add(
                        SchemaMutation(
                            column_name=col,
                            attribute=attribute,
                            value_old=str(schema_old[attribute]),
                            value_new=str(schema_new[attribute]),
                        )
                    )
        return schema_mutations

    @cached_property
    def schema_mutations_type(self) -> set[str]:
        "Detail for schema mutations of 'data_type' attribute"
        return set([sm for sm in self.schema_mutations if sm.attribute == "type"])

    @cached_property
    def schema_mutations_nullable(self) -> set[str]:
        "Detail for schema mutations of 'nullable' attribute"
        return set([sm for sm in self.schema_mutations if sm.attribute == "nullable"])

    @cached_property
    def schema_mutations_metadata(self) -> set[str]:
        "Detail for schema mutations of 'metadata' attribute"
        return set([sm for sm in self.schema_mutations if sm.attribute == "metadata"])

    @cached_property
    def columns_changed_type(self) -> set[str]:
        "Columns in both `df_old` and `df_new` whose 'data_type' attribute changed"
        return set([sm.column_name for sm in self.schema_mutations_type])

    @cached_property
    def columns_changed_nullable(self) -> set[str]:
        "Columns in both `df_old` and `df_new` whose 'nullable' attribute changed"
        return set([sm.column_name for sm in self.schema_mutations_nullable])

    @cached_property
    def columns_changed_metadata(self) -> set[str]:
        "Columns in both `df_old` and `df_new` whose 'metadata' attribute changed"
        return set([sm.column_name for sm in self.schema_mutations_metadata])

    @cached_property
    def columns_comparable(self) -> set[str]:
        """
        Columns that can be compared for regression test.

        Comparable columns must (1) exist in `df_old` and `df_new`, and (2) have the same data_type in `df_old` and `df_new`
        """
        return self.columns_kept.difference(self.columns_changed_type)

    # Table info
    # ---------------------------------------------------------------------------------
    @cached_property
    def count_record_old(self) -> int:
        "Count of records in `df_old`"
        return self.df_old.count()

    @cached_property
    def count_record_new(self) -> int:
        "Count of records in `df_new`"
        return self.df_new.count()

    @cached_property
    def count_pk_old(self) -> int:
        "Count of primary keys in `df_old`"
        return self.df_old.select(F.col("pk")).distinct().count()

    @cached_property
    def count_pk_new(self) -> int:
        "Count of primary keys in `df_new`"
        return self.df_new.select(F.col("pk")).distinct().count()

    # Duplicates
    # ---------------------------------------------------------------------------------
    @staticmethod
    def _df_duplicate(df: DataFrame, sort_samples: bool) -> DataFrame:
        df_result = (
            df.select(F.col("pk"))
            .groupBy(F.col("pk"))
            .agg((F.count(F.col("pk")) - 1).alias("count_record_duplicate"))
            .filter(F.col("count_record_duplicate") > 0)
        )
        if sort_samples:
            return df_result.orderBy([F.col("count_record_duplicate").desc(), F.col("pk")])
        return df_result

    @property
    def df_duplicate_old(self) -> DataFrame:
        "DataFrame containing all duplicate primary keys (and their counts) in `df_old`"
        df_cached = self._df_cache.get("df_duplicate_old")
        if df_cached:
            return df_cached
        df_duplicate_old = self._df_duplicate(self.df_old, sort_samples=self.sort_samples)

        self._df_cache["df_duplicate_old"] = df_duplicate_old.checkpoint(eager=False)
        return df_duplicate_old

    @property
    def df_duplicate_new(self) -> DataFrame:
        "DataFrame containing all duplicate primary keys (and their counts) in `df_new`"
        df_cached = self._df_cache.get("df_duplicate_new")
        if df_cached:
            return df_cached
        df_duplicate_new = self._df_duplicate(self.df_new, sort_samples=self.sort_samples)

        self._df_cache["df_duplicate_new"] = df_duplicate_new.checkpoint(eager=False)
        return df_duplicate_new

    @staticmethod
    def _count_record_duplicate(df_duplicate: DataFrame) -> int:
        df_result = df_duplicate.agg(F.sum(F.col("count_record_duplicate")).alias("crd"))
        result = [row.crd for row in df_result.collect()]
        return result[0] or 0

    @cached_property
    def count_duplicate_record_old(self) -> int:
        "Count of duplicate records in `df_old`"
        return self._count_record_duplicate(self.df_duplicate_old)

    @cached_property
    def count_duplicate_record_new(self) -> int:
        "Count of duplicate records in `df_new`"
        return self._count_record_duplicate(self.df_duplicate_new)

    @cached_property
    def count_duplicate_pk_old(self) -> int:
        "Count of duplicate primary keys in `df_old`"
        return self.df_duplicate_old.count() or 0

    @cached_property
    def count_duplicate_pk_new(self) -> int:
        "Count of duplicate primary keys in `df_new`"
        return self.df_duplicate_new.count() or 0

    @staticmethod
    def _sample_duplicate_pk(df_duplicate: DataFrame, num_samples: int) -> tuple:
        df_result = df_duplicate.select(F.col("pk")).limit(num_samples)
        return tuple([row.pk for row in df_result.collect()])

    @cached_property
    def sample_duplicate_pk_old(self) -> tuple:
        "A sample of duplicate primary keys in `df_old`"
        return self._sample_duplicate_pk(self.df_duplicate_old, num_samples=self.num_samples)

    @cached_property
    def sample_duplicate_pk_new(self) -> tuple:
        "A sample of duplicate primary keys in `df_new`"
        return self._sample_duplicate_pk(self.df_duplicate_new, num_samples=self.num_samples)

    @cached_property
    def has_symmetric_duplicates(self) -> bool:
        """
        If True:
        1. `df_old` and `df_new` both have duplicates.
        2. Those duplicates are exactly the same".
        """
        flag = False
        if self.count_duplicate_record_old > 0:
            df_old_dups_only = self.df_old.join(self.df_duplicate_old, how="left_semi", on=["pk"]).select(*self.columns_comparable)
            df_new_dups_only = self.df_new.join(self.df_duplicate_new, how="left_semi", on=["pk"]).select(*self.columns_comparable)
            old_minus_new = df_old_dups_only.exceptAll(df_new_dups_only)
            new_minus_old = df_new_dups_only.exceptAll(df_old_dups_only)
            if old_minus_new.unionAll(new_minus_old).count() == 0:
                flag = True
        return flag

    # Orphans
    # ---------------------------------------------------------------------------------
    @staticmethod
    def _df_orphan(df: DataFrame, df_anti: DataFrame, sort_samples: bool) -> DataFrame:
        df_results = df.join(df_anti, how="left_anti", on=["pk"]).select(F.col("pk")).distinct()
        if sort_samples:
            return df_results.orderBy(F.col("pk"))
        return df_results

    @property
    def df_orphan_old(self) -> DataFrame:
        "DataFrame containing primary keys in `df_old` that are not in `df_new`"
        df_cached = self._df_cache.get("df_orphan_old")
        if df_cached:
            return df_cached
        df_orphan_old = self._df_orphan(df=self.df_old, df_anti=self.df_new, sort_samples=self.sort_samples)

        self._df_cache["df_orphan_old"] = df_orphan_old.checkpoint(eager=False)
        return df_orphan_old

    @property
    def df_orphan_new(self) -> DataFrame:
        "DataFrame containing primary keys in `df_new` that are not in `df_old`"
        df_cached = self._df_cache.get("df_orphan_new")
        if df_cached:
            return df_cached
        df_orphan_new = self._df_orphan(df=self.df_new, df_anti=self.df_old, sort_samples=self.sort_samples)

        self._df_cache["df_orphan_new"] = df_orphan_new.checkpoint(eager=False)
        return df_orphan_new

    @cached_property
    def count_orphan_pk_old(self) -> int:
        "Count of primary keys in `df_old` that are not in `df_new`"
        return self.df_orphan_old.count() or 0

    @cached_property
    def count_orphan_pk_new(self) -> int:
        "Count of primary keys in `df_new` that are not in `df_old`"
        return self.df_orphan_new.count() or 0

    @staticmethod
    def _sample_orphan_pk(df: DataFrame, num_samples: int) -> tuple:
        df_results = df.select(F.col("pk")).limit(num_samples)
        results = [row.pk for row in df_results.collect()]
        return tuple(results)

    @cached_property
    def sample_orphan_pk_old(self) -> tuple:
        "A sample of primary keys in `df_old` that are not in `df_new`"
        return self._sample_orphan_pk(self.df_orphan_old, num_samples=self.num_samples)

    @cached_property
    def sample_orphan_pk_new(self) -> tuple:
        "A sample of primary keys in `df_new` that are not in `df_old`"
        return self._sample_orphan_pk(self.df_orphan_new, num_samples=self.num_samples)

    # Regression
    # ---------------------------------------------------------------------------------
    @cached_property
    def df_comparable(self) -> DataFrame:
        """
        DataFrame of rows that are eligible for regression testing.

        Rows are eligible if they:
        1. Are not duplicates
        2. Are not orphans
        3. Have matching primary keys between `df_old` and `df_new`
        """
        df_cached = self._df_cache.get("df_comparable")
        if df_cached:
            return df_cached

        df_old_dups_none = self.df_old.join(self.df_duplicate_old, how="left_anti", on=["pk"]).select(["pk"] + list(self.columns_comparable))
        df_new_dups_none = self.df_new.join(self.df_duplicate_new, how="left_anti", on=["pk"]).select(["pk"] + list(self.columns_comparable))

        def prefix_columns(df: DataFrame, prefix: str) -> DataFrame:
            return df.select([F.col(c).alias(c if c == "pk" else f"{prefix}_{c}") for c in df.columns])

        df_old_dups_none_renamed = prefix_columns(df=df_old_dups_none, prefix="old")
        df_new_dups_none_renamed = prefix_columns(df=df_new_dups_none, prefix="new")
        df_comparable = df_old_dups_none_renamed.join(df_new_dups_none_renamed, on=["pk"])

        self._df_cache["df_comparable"] = df_comparable.checkpoint(eager=False)
        return df_comparable

    @cached_property
    def count_comparable_record(self) -> int:
        "Count of records that are eligible for regression testing"
        return self.df_comparable.count()

    @cached_property
    def count_comparable_pk(self) -> int:
        "Count of primary keys that are eligible for regression testing"
        return self.df_comparable.select(F.col("pk")).distinct().count()

    @property
    def df_regression(self) -> DataFrame:
        """
        DataFrame that:
        1. Joins `df_old` and `df_new` on `pk`
        2. Filters out duplicates
        3. Filters for at least one column with diffs
        """
        df_cached = self._df_cache.get("df_regression")
        if df_cached:
            return df_cached

        case_sql = list()
        where_sql = list()
        for c in self.columns_comparable:
            oc = "old_" + c
            nc = "new_" + c
            case_sql.append(
                f"CASE WHEN ((`{oc}` != `{nc}`) OR (`{oc}` IS NULL AND `{nc}` IS NOT NULL) OR (`{oc}` IS NOT NULL AND `{nc}` IS NULL)) THEN '{c}' ELSE NULL END"
            )
            where_sql.append(f"((`{oc}` != `{nc}`) OR (`{oc}` IS NULL AND `{nc}` IS NOT NULL) OR (`{oc}` IS NOT NULL AND `{nc}` IS NULL))")
        case_expr = f"SPLIT(CONCAT_WS(',', {','.join(case_sql)}), ',')"
        where_expr = " OR ".join(where_sql)
        df_regression = self.df_comparable.filter(F.expr(where_expr)).withColumn("diff_cols", F.expr(case_expr))
        self._df_cache["df_regression"] = df_regression.checkpoint(eager=False)
        return df_regression

    @cached_property
    def count_diff_record(self) -> int:
        "Count of records with diffs"
        return self.df_regression.count() or 0

    @cached_property
    def count_diff_pk(self) -> int:
        "Count of primary keys with diffs"
        return self.df_regression.select(F.col("pk")).distinct().count() or 0

    @cached_property
    def df_diff_cols(self) -> DataFrame:
        "DataFrame of columns that contain diffs"
        df_cache = self._df_cache.get("df_diff_cols")
        if df_cache:
            return df_cache

        df_diff_cols = self.df_regression.select(F.explode(F.col("diff_cols")).alias("diff_cols")).distinct()

        self._df_cache["df_diff_cols"] = df_diff_cols.checkpoint(eager=False)
        return df_diff_cols

    @cached_property
    def columns_diff(self) -> set[str]:
        "Columns that contain diffs"
        return set([row.diff_cols for row in self.df_diff_cols.collect()])

    @staticmethod
    def _col_diff_category(old_col: Column, new_col: Column, data_type: str) -> Column:
        col_default = F.lit("uncategorized")
        if data_type == "string":
            col_diff_category = (
                F.when(
                    (F.trim(new_col) == old_col) & (F.ltrim(new_col) != old_col) & (F.rtrim(new_col) != old_col),
                    F.lit("padding added (left and right)"),
                )
                .when(
                    (F.trim(old_col) == new_col) & (F.ltrim(old_col) != new_col) & (F.rtrim(old_col) != new_col),
                    F.lit("padding removed (left and right)"),
                )
                .when(F.ltrim(new_col) == old_col, F.lit("padding added (left)"))
                .when(F.rtrim(new_col) == old_col, F.lit("padding added (right)"))
                .when(F.ltrim(old_col) == new_col, F.lit("padding removed (left)"))
                .when(F.rtrim(old_col) == new_col, F.lit("padding removed (right)"))
                .when(F.upper(old_col) == new_col, F.lit("capitalization added"))
                .when(F.lower(new_col) == old_col, F.lit("capitalization added"))
                .when(F.upper(new_col) == old_col, F.lit("capitalization removed"))
                .when(F.lower(old_col) == new_col, F.lit("capitalization removed"))
                .when(F.upper(old_col) == F.upper(new_col), F.lit("capitalization changed"))
                .when(old_col.startswith(new_col), F.lit("truncation added"))
                .when(new_col.startswith(old_col), F.lit("truncation removed"))
                .otherwise(col_default)
            )
        elif data_type in ["float", "double", "decimal"]:
            col_diff_category = (
                F.when(F.round(old_col, 2) == F.round(new_col, 2), F.lit("rounding"))
                .when(new_col % old_col == F.lit(0), "multiple")
                .when(old_col % new_col == F.lit(0), "divisible")
                .otherwise(col_default)
            )
        elif data_type == "timestamp":
            col_diff_category = (
                F.when(
                    F.abs(old_col.cast(LongType()) - new_col.cast(LongType())) % 3600 == 0,
                    F.lit("hour shift"),
                )
                .when(
                    F.date_trunc("millisecond", old_col) == F.date_trunc("millisecond", new_col),
                    F.lit("millisecond truncation"),
                )
                .otherwise(col_default)
            )
        elif data_type == "boolean":
            col_diff_category = (
                F.when(old_col & ~new_col, "boolean flip (true -> false)").when(new_col & ~old_col, "boolean flip (false -> true)").otherwise(col_default)
            )
        else:
            col_diff_category = col_default

        return (
            F.when(old_col.isNull() & new_col.isNotNull(), F.lit("null flip (null -> not null)"))
            .when(old_col.isNotNull() & new_col.isNull(), F.lit("null flip (not null -> null)"))
            .otherwise(col_diff_category)
        )

    @staticmethod
    def _quote_if_string(col: Column, data_type: str) -> Column:
        if data_type == "string":
            return F.concat(F.lit("'"), col, F.lit("'"))
        return col.cast(StringType())

    @cached_property
    def df_diff(self) -> DataFrame:
        "DataFrame that pivots and filters `df_regression` to show one row, for every row and column containing values that are different"
        df_cache = self._df_cache.get("df_diff")
        if df_cache:
            return df_cache

        dfs = list()
        schema = self.df_old.schema
        for col in self.columns_diff:
            data_type = schema[col].dataType.typeName()
            df = self.df_regression.filter(F.array_contains(F.col("diff_cols"), F.lit(col))).select(
                F.lit(col).alias("column_name"),
                F.lit(data_type).alias("data_type"),
                F.col("pk").alias("pk"),
                self._quote_if_string(col=F.col(f"old_{col}"), data_type=data_type).alias("old_value"),
                self._quote_if_string(col=F.col(f"new_{col}"), data_type=data_type).alias("new_value"),
                self._col_diff_category(old_col=F.col(f"old_{col}"), new_col=F.col(f"new_{col}"), data_type=data_type).alias("diff_category"),
            )
            df.checkpoint(eager=False)
            dfs.append(df)

        df_diff = functools.reduce(DataFrame.unionAll, dfs)
        self._df_cache["df_diff"] = df_diff.checkpoint(eager=False)
        return df_diff

    @cached_property
    def df_diff_summary(self) -> DataFrame:
        "A transformation of `df_diff` that aggregates by `column_name` and `diff_category`"
        df_cache = self._df_cache.get("df_diff_summary")
        if df_cache:
            return df_cache

        df_diff_summary = (
            self.df_diff.groupBy(F.col("column_name"), F.col("data_type"), F.col("diff_category"))
            .agg(F.count("pk").alias("count_record"))
            .orderBy(F.col("column_name"), F.col("diff_category"))
            .withColumn(
                "count_record_%oT",
                F.concat(
                    F.round(F.col("count_record") / F.lit(self.count_comparable_record) * 100, 1).cast(StringType()),
                    F.lit("%"),
                ),
            )
        )

        self._df_cache["df_diff_summary"] = df_diff_summary.checkpoint(eager=False)
        return df_diff_summary

    @cached_property
    def df_diff_sample(self) -> DataFrame:
        "A sample of diffs for every `column_name`, for every `diff_category`"
        df_cache = self._df_cache.get("df_diff_sample")
        if df_cache:
            return df_cache

        df_diff_sample = (
            self.df_diff.withColumn(
                "rn",
                F.row_number().over(Window.partitionBy([F.col("column_name"), F.col("diff_category")]).orderBy(F.col("pk"))),
            )
            .filter(F.col("rn") <= F.lit(self.num_samples))
            .drop("rn")
            .orderBy(F.col("column_name"), F.col("diff_category"), F.col("pk"))
        )

        self._df_cache["df_diff_sample"] = df_diff_sample.checkpoint(eager=False)
        return df_diff_sample

    # Summary
    # ---------------------------------------------------------------------------------
    @property
    def is_success(self) -> bool:
        """
        If True, the regression test was successful.

        This property is False when:
        - Columns are removed
        - Columns change data_type
        - Duplicates exist (and are not symmetrical)
        - Orphans exist
        - Diffs exist
        """
        if self.columns_removed:
            return False
        elif self.columns_changed_type:
            return False
        elif self.count_duplicate_pk_old > 0 and not self.has_symmetric_duplicates:
            return False
        elif self.count_orphan_pk_old > 0 or self.count_orphan_pk_new > 0:
            return False
        elif self.count_diff_pk > 0:
            return False
        else:
            return True

    @staticmethod
    def _duration_format(duration_seconds: int) -> str:
        "The duration of the Regression Test, as a formatted string."
        if duration_seconds < 60:
            return f"{duration_seconds} second(s)"
        elif duration_seconds < 60 * 60:
            s = duration_seconds % 60
            m = int(duration_seconds / 60)
            return f"{m} minute(s), {s} second(s)"
        else:
            s = duration_seconds % 60
            m = duration_seconds % (60 * 60)
            h = int(duration_seconds / (60 * 60))
            return f"{h} hour(s), {m} minute(s), {s} second(s)"

    @cached_property
    def summary(self) -> str:
        "A Markdown string that summarizes the regression test results."
        run_started_at = datetime.now()
        if self.is_success:
            run_stopped_at = datetime.now()
            duration_formatted = self._duration_format((run_stopped_at - run_started_at).total_seconds())
            return "\n".join(
                [
                    "\n## Regression Test for table '{self.table_name}': SUCCESS",
                    f"\n(ran in {duration_formatted})",
                ]
            )

        summary: list[str] = list()
        summary.append(f"\n## Regression Test for table '{self.table_name}': FAILURE")
        summary.append(f"- run_id: {self.run_id}")

        summary.append("\n### Table stats")
        summary.append(f"- pk: {self.pk}")
        summary.append(f"- Count records in old {self.table_name}: {self.count_record_old}")
        summary.append(f"- Count records in new {self.table_name}: {self.count_record_new}")
        summary.append(f"- Count pks in old {self.table_name}: {self.count_pk_old}")
        summary.append(f"- Count pks in new {self.table_name}: {self.count_pk_new}")

        if self.columns_added or self.columns_removed:
            summary.append("\n### Column Changes")
            if self.columns_added:
                summary.append(f"- Columns Added: {list(self.columns_added)}")
            if self.columns_removed:
                summary.append(f"- Columns Removed: {list(self.columns_removed)}")

        if self.schema_mutations:
            summary.append("\n### Schema Mutations")
            for sm in self.schema_mutations:
                summary.append(f"- For column '{sm.column_name}', attribute '{sm.attribute}' changed from '{sm.value_old}' to '{sm.value_new}'.")

        if self.count_duplicate_record_old or self.count_duplicate_record_new:
            summary.append("\n### Duplicates")
            summary.append(
                f"- Count of duplicate records in old {self.table_name}: {self.count_duplicate_record_old} (%oT: {(self.count_duplicate_record_old / self.count_record_old):.1%})"
            )  # noqa: E501
            summary.append(
                f"- Count of duplicate records in new {self.table_name}: {self.count_duplicate_record_new} (%oT: {(self.count_duplicate_record_new / self.count_record_new):.1%})"
            )  # noqa: E501
            summary.append(
                f"- Count of duplicate pks in old {self.table_name}: {self.count_duplicate_pk_old} (%oT: {(self.count_duplicate_pk_old / self.count_pk_old):.1%})"
            )  # noqa: E501
            summary.append(
                f"- Count of duplicate pks in new {self.table_name}: {self.count_duplicate_pk_new} (%oT: {(self.count_duplicate_pk_new / self.count_pk_new):.1%})"
            )  # noqa: E501
            summary.append(f"- Sample of duplicate pks in old {self.table_name}: {[str(sample) for sample in self.sample_duplicate_pk_old]}")
            summary.append(f"- Sample of duplicate pks in new {self.table_name}: {[str(sample) for sample in self.sample_duplicate_pk_new]}")
            if self.has_symmetric_duplicates:
                summary.append("**NOTE: Duplicates are exactly the same between `df_old` and `df_new`.**")

        if self.count_orphan_pk_old or self.count_orphan_pk_new:
            summary.append("\n### Orphans")
            summary.append(
                f"- Count of orphan pks in old {self.table_name}: {self.count_orphan_pk_old} (%oT: {(self.count_orphan_pk_old / self.count_pk_old):.1%})"
            )  # noqa: E501
            summary.append(
                f"- Count of orphan pks in new {self.table_name}: {self.count_orphan_pk_new} (%oT: {(self.count_orphan_pk_new / self.count_pk_new):.1%})"
            )  # noqa: E501
            summary.append(f"- Sample of orphan pks in old {self.table_name}: {[str(sample) for sample in self.sample_orphan_pk_old]}")
            summary.append(f"- Sample of orphan pks in new {self.table_name}: {[str(sample) for sample in self.sample_orphan_pk_new]}")

        if self.count_diff_record:
            summary.append("\n### Diffs")
            summary.append(f"- Columns with diffs: {self.columns_diff}")
            summary.append(f"- Number of records with diffs: {self.count_diff_record} (%oT: {(self.count_diff_record / self.count_comparable_record):.1%})")

            summary.append("\n#### Diff Summary\n")
            summary.append(tabulate(self.df_diff_summary.toPandas(), headers="keys", missingval="NULL", tablefmt="pipe", showindex=False))

            summary.append("\n#### Diff Samples\n")
            summary.append(tabulate(self.df_diff_sample.toPandas(), headers="keys", missingval="NULL", tablefmt="pipe", showindex=False))

        run_stopped_at = datetime.now()
        duration_formatted = self._duration_format((run_stopped_at - run_started_at).total_seconds())
        summary.append(f"\n(ran in {duration_formatted})\n")

        return "\n".join(summary)
