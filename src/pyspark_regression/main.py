import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property, reduce
from time import perf_counter
from typing import List, Set
from uuid import UUID, uuid4

from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import LongType, StringType, BooleanType, TimestampType, FloatType, DoubleType


logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class RegressionTest:
    """
    A class with methods to compare differences between two Spark Dataframes.

    Args:
        df_old (DataFrame): Old DataFrame
        df_new (DataFrame): New DataFrame
        pk (str | Column): Primary Key shared between df_old and df_new. Can be either a string or a Spark Column.
        table_name (str = 'df'): Name of table
        num_samples (int = 5): Number of records to pull when retrieving samples.
    Returns:
        RegressionTest
    """
    spark: SparkSession
    "Spark Session"
    df_old: DataFrame
    "Old DataFrame"
    df_new: DataFrame
    "New DataFrame"
    pk: str
    "Primary Key shared between df_old and df_new. Can be either a string or a Spark Column."
    table_name: str = 'df'
    "Name of table"
    num_samples: int = 5
    "Number of records to pull when retrieving samples."
    run_id: UUID = uuid4()
    "A unique id for the Regression Test"
    run_time: datetime = datetime.now()
    "Time at which the Regression Test was initialized"

    def __post_init__(self):

        if self.pk not in self.df_old.columns:
            raise KeyError(f"pk '{self.pk}' is missing from df_old.")
        if self.pk not in self.df_new.columns:
            raise KeyError(f"pk '{self.pk}' is missing from df_new.")

        for reserved_column_name in ["pk", "diff_cols"]:
            if reserved_column_name in self.df_old.columns:
                raise KeyError(f"The column name '{reserved_column_name}' itself is reserved. Please remove it from df_old.")
            if reserved_column_name in self.df_new.columns:
                raise KeyError(f"The column name '{reserved_column_name}' itself is reserved. Please remove it from df_new.")

        # Add pk to df_old and df_new
        object.__setattr__(self, 'df_old', self.df_old.withColumn('pk', F.col(self.pk)).orderBy('pk'))
        object.__setattr__(self, 'df_new', self.df_new.withColumn('pk', F.col(self.pk)).orderBy('pk'))

    # Schema Analysis
    # -------------------------------------------------------------------------
    @cached_property
    def columns_old(self) -> Set[str]:
        "Columns in df_old"
        return set([col for col in self.df_old.columns if not col == 'pk'])

    @cached_property
    def columns_new(self) -> Set[str]:
        "Columns in df_new"
        return set([col for col in self.df_new.columns if not col == 'pk'])

    @cached_property
    def columns_all(self) -> Set[str]:
        "All columns between df_old and df_new"
        return self.columns_new.union(self.columns_old)

    @cached_property
    def columns_added(self) -> Set[str]:
        "Columns in df_new that are not in df_old"
        return self.columns_new.difference(self.columns_old)

    @cached_property
    def columns_removed(self) -> Set[str]:
        "Columns in df_old that are not in df_new"
        return self.columns_old.difference(self.columns_new)

    @cached_property
    def columns_kept(self) -> Set[str]:
        "Columns that are in both df_old and df_new"
        return self.columns_old.intersection(self.columns_new)

    @cached_property
    def schema_mutations(self) -> Set[SchemaMutation]:
        "Detail for all schema mutations between df_old and df_new"
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
    def schema_mutations_type(self) -> Set[str]:
        "Detail for schema mutations of 'data_type' attribute"
        return set([sm for sm in self.schema_mutations if sm.attribute == 'type'])

    @cached_property
    def schema_mutations_nullable(self) -> Set[str]:
        "Detail for schema mutations of 'nullable' attribute"
        return set([sm for sm in self.schema_mutations if sm.attribute == 'nullable'])

    @cached_property
    def schema_mutations_metadata(self) -> Set[str]:
        "Detail for schema mutations of 'metadata' attribute"
        return set([sm for sm in self.schema_mutations if sm.attribute == 'metadata'])

    @cached_property
    def columns_changed_type(self) -> Set[str]:
        "Columns in both df_old and df_new for which their 'data_type' attribute changed"
        return set([sm.column_name for sm in self.schema_mutations_type])

    @cached_property
    def columns_changed_nullable(self) -> Set[str]:
        "Columns in both df_old and df_new for which their 'nullable' attribute changed"
        return set([sm.column_name for sm in self.schema_mutations_nullable])

    @cached_property
    def columns_changed_metadata(self) -> Set[str]:
        "Columns in both df_old and df_new for which their 'metadata' attribute changed"
        return set([sm.column_name for sm in self.schema_mutations_metadata])

    @cached_property
    def columns_comparable(self) -> Set[str]:
        """
        Columns that can be compared for regression test. Comparable columns (1) must be
        present in df_old and df_new, and (2) have the same data_type in df_old and df_new
        """
        return self.columns_kept.difference(self.columns_changed_type)
    # -------------------------------------------------------------------------

    # Base Table Info
    # -------------------------------------------------------------------------
    @cached_property
    def count_record_old(self) -> int:
        "Count of records in df_old"
        return self.df_old.count()

    @cached_property
    def count_record_new(self) -> int:
        "Count of records in df_new"
        return self.df_new.count()

    @cached_property
    def count_pk_old(self) -> int:
        "Count of pks in df_old"
        return self.df_old.select(F.col('pk')).distinct().count()

    @cached_property
    def count_pk_new(self) -> int:
        "Count of pks in df_new"
        return self.df_new.select(F.col('pk')).distinct().count()
    # -------------------------------------------------------------------------

    # Duplicate Analysis
    # -------------------------------------------------------------------------
    @cached_property
    def df_duplicate_old(self) -> DataFrame:
        "DataFrame of duplicate pks in old table, with count of duplicates"
        return (
            self.df_old
            .select(F.col('pk'))
            .groupBy(F.col('pk'))
            .agg((F.count(F.col('pk')) - 1).alias('count_record_duplicate'))
            .filter(F.col('count_record_duplicate') > 0)
        )

    @cached_property
    def df_duplicate_new(self) -> DataFrame:
        "DataFrame of duplicate pks in new table, with count of duplicates"
        return (
            self.df_new
            .select(F.col('pk'))
            .groupBy(F.col('pk'))
            .agg((F.count(F.col('pk')) - 1).alias('count_record_duplicate'))
            .filter(F.col('count_record_duplicate') > 0)
        )

    @cached_property
    def count_record_duplicate_old(self) -> int:
        "Count of duplicate records in df_old"
        return self.df_duplicate_old.agg(F.sum(F.col('count_record_duplicate')).alias('crd')).collect()[0][0] or 0

    @cached_property
    def count_record_duplicate_new(self) -> int:
        "Count of duplicate records in df_new"
        return self.df_duplicate_new.agg(F.sum(F.col('count_record_duplicate')).alias('crd')).collect()[0][0] or 0

    @cached_property
    def count_pk_duplicate_old(self) -> int:
        "Count of pks that have duplicates in df_old"
        return self.df_duplicate_old.count() or 0

    @cached_property
    def count_pk_duplicate_new(self) -> int:
        "Count of pks that have duplicates in df_new"
        return self.df_duplicate_new.count() or 0

    @cached_property
    def sample_pk_duplicate_old(self) -> tuple:
        "num_sample samples of pks that have duplicate records in df_old"
        return tuple(
            [
                row.pk
                for row in (
                    self.df_duplicate_old
                    .orderBy([F.col('count_record_duplicate').desc(), F.col('pk')]) # sort worst offenders first, then pk
                    .select(F.col('pk'))
                    .limit(self.num_samples).collect()
                )
            ]
        )

    @cached_property
    def sample_pk_duplicate_new(self) -> tuple:
        "num_sample samples of pks that have duplicate records in df_new"
        return tuple(
            [
                row.pk
                for row in (
                    self.df_duplicate_new
                    .orderBy([F.col('count_record_duplicate').desc(), F.col('pk')]) # sort worst offenders first, then pk
                    .select(F.col('pk'))
                    .limit(self.num_samples).collect()
                )
            ]
        )

    @cached_property
    def has_symmetric_duplicates(self) -> bool:
        "True if duplicates in df_old and df_new are exactly the same."
        if self.count_record_duplicate_old == 0: # Can't have symmetric duplicates without at least one duplicate
            return False
        __df_dup_old_comp = self.df_old.join(self.df_duplicate_old, how='left_semi', on=['pk']).select(list(self.columns_comparable))
        __df_dup_new_comp = self.df_new.join(self.df_duplicate_new, how='left_semi', on=['pk']).select(list(self.columns_comparable))
        return (__df_dup_old_comp.exceptAll(__df_dup_new_comp)).unionAll(__df_dup_new_comp.exceptAll(__df_dup_old_comp)).count() == 0
    # -------------------------------------------------------------------------

    # Orphan Analysis
    # -------------------------------------------------------------------------
    @cached_property
    def df_orphan_old(self) -> DataFrame:
        "DataFrame of orphan pks in old table"
        return self.df_old.join(self.df_new.hint('merge'), how='left_anti', on=['pk']).select(F.col('pk')).distinct()

    @cached_property
    def df_orphan_new(self) -> DataFrame:
        "DataFrame of orphan pks in new table"
        return self.df_new.join(self.df_old.hint('merge'), how='left_anti', on=['pk']).select(F.col('pk')).distinct()

    @cached_property
    def count_pk_orphan_old(self) -> int:
        "Count of pks that are in df_old but not df_new"
        return self.df_orphan_old.count() or 0

    @cached_property
    def count_pk_orphan_new(self) -> int:
        "Count of pks that are in df_new but not df_old"
        return self.df_orphan_new.count() or 0

    @cached_property
    def sample_pk_orphan_old(self) -> tuple:
        "num_sample samples of pks that are in df_old but not df_new"
        return tuple([row.pk for row in self.df_orphan_old.select(F.col('pk')).orderBy(F.col('pk')).limit(self.num_samples).collect()])

    @cached_property
    def sample_pk_orphan_new(self) -> tuple:
        "num_sample samples of pks that are in df_new but not df_old"
        return tuple([row.pk for row in self.df_orphan_new.select(F.col('pk')).orderBy(F.col('pk')).limit(self.num_samples).collect()])
    # -------------------------------------------------------------------------

    # Diff Analysis
    # -------------------------------------------------------------------------
    @cached_property
    def df_comparable(self) -> DataFrame:
        df_old_no_dups = self.df_old.join(self.df_duplicate_old, how='left_anti', on=["pk"])
        df_new_no_dups = self.df_new.join(self.df_duplicate_new, how='left_anti', on=["pk"])
        column_to_data_type = {s.name: s.dataType.typeName() for s in self.df_old.schema}
        df_comparable = (
            df_old_no_dups.alias('o')
            .join(df_new_no_dups.alias('n'), on=(F.col('o.pk') == F.col('n.pk')))
            .select(
                F.col("o.pk"),
                F.array(
                    [
                        F.create_map(
                            F.lit("column_name"),
                            F.lit(c),
                            F.lit("data_type"),
                            F.lit(column_to_data_type[c]),
                            F.lit("old_value"),
                            F.col(f"o.{c}").cast(StringType()),
                            F.lit("new_value"),
                            F.col(f"n.{c}").cast(StringType()),
                        ) for c in self.columns_comparable
                    ]
                ).alias("column_map"),
            )
        )

        return df_comparable

    @cached_property
    def count_pk_comparable(self) -> int:
        "Count of comparable records between df_old and df_new"
        return self.df_comparable.count()

    @cached_property
    def df_regression(self):
        df_regression = (
            self.df_comparable
            .select(
                F.col("pk"),
                F.expr(
                    """
                    FILTER(
                        column_map,
                        column -> (
                            (column.old_value != column.new_value)
                            OR (column.old_value IS NULL AND column.new_value IS NOT NULL)
                            OR (column.old_value IS NOT NULL AND column.new_value IS NULL)
                        )
                    )
                    """
                ).alias("diffs"),
            )
            .filter(F.size(F.col("diffs")) > 0)
        )
        return df_regression

    @cached_property
    def count_record_diff(self) -> int:
        "Count of records with diffs"
        return self.df_regression.count() or 0

    @cached_property
    def count_pk_diff(self) -> int:
        "Count of pks with diffs"
        return self.df_regression.select(F.col("pk")).distinct().count() or 0

    @cached_property
    def df_diff_cols(self) -> DataFrame:
        return (
            self.df_regression
            .select(F.explode(F.col("diffs").column_name).alias("diff_cols"))
            .distinct()
            .orderBy(F.col("diff_cols"))
        )

    @cached_property
    def columns_diff(self) -> Set[str]:
        "Columns containing at least one value difference between df_old and df_new"
        return set([row.diff_cols for row in self.df_diff_cols.collect()])

    @staticmethod
    def __quote_if_string(col: Column, data_type: Column) -> Column:
        return F.when(data_type == F.lit("string"), F.concat(F.lit("'"), col, F.lit("'"))).otherwise(col)

    @staticmethod
    def __diff_category(x: Column, y: Column, data_type: Column) -> Column:
        """
        Categorizes the difference between two columns.
        """
        col_default = F.lit('uncategorized')
        return (
            F.when(x.isNull() & y.isNotNull(), F.lit('null flip (null -> not null)'))
            .when(x.isNotNull() & y.isNull(), F.lit('null flip (not null -> null)'))
            .when((x == F.lit('None')) & y.isNull(), F.lit("'None' flip ('None' -> null)"))
            .when(x.isNull() & (y == F.lit('None')), F.lit("'None' flip (null -> 'None')"))
            .when((x == F.lit('None')) & y.isNotNull(), F.lit("'None' flip ('None' -> not null)"))
            .when(x.isNotNull() & (y == F.lit('None')), F.lit("'None' flip (not null -> 'None')"))
            .otherwise(
                F.when(
                    data_type == F.lit('string'),
                    F.when((F.trim(y) == x) & (F.ltrim(y) != x) & (F.rtrim(y) != x), F.lit('padding added (left and right)'))
                    .when((F.trim(x) == y) & (F.ltrim(x) != y) & (F.rtrim(x) != y), F.lit('padding removed (left and right)'))
                    .when(F.ltrim(y) == x, F.lit('padding added (left)'))
                    .when(F.rtrim(y) == x, F.lit('padding added (right)'))
                    .when(F.ltrim(x) == y, F.lit('padding removed (left)'))
                    .when(F.rtrim(x) == y, F.lit('padding removed (right)'))
                    .when(F.upper(x) == y, F.lit('capitalization added'))
                    .when(F.lower(y) == x, F.lit('capitalization added'))
                    .when(F.upper(y) == x, F.lit('capitalization removed'))
                    .when(F.lower(x) == y, F.lit('capitalization removed'))
                    .when(F.upper(x) == F.upper(y), F.lit('capitalization changed'))
                    .when(x.startswith(y), F.lit('truncation added'))
                    .when(y.startswith(x), F.lit('truncation removed'))
                    .otherwise(col_default)
                )
                .when(
                    data_type == F.lit('float'),
                    F.when(F.round(x.cast(FloatType()), 2) == F.round(y.cast(FloatType()), 2), F.lit('rounding'))
                    .when(x.cast(FloatType()) % y.cast(FloatType()) == F.lit(0), F.lit('multiple'))
                    .when(y.cast(FloatType()) % x.cast(FloatType()) == F.lit(0), F.lit('multiple'))
                    .otherwise(col_default)
                )
                .when(
                    # Because I cant know the precision, use double for decimal
                    data_type.isin('double', 'decimal'),
                    F.when(F.round(x.cast(DoubleType()), 2) == F.round(y.cast(DoubleType()), 2), F.lit('rounding'))
                    .when(x.cast(DoubleType()) % y.cast(DoubleType()) == F.lit(0), F.lit('multiple'))
                    .when(y.cast(DoubleType()) % x.cast(DoubleType()) == F.lit(0), F.lit('multiple'))
                    .otherwise(col_default)
                )
                .when(
                    data_type == F.lit('timestamp'),
                    F.when(F.abs(x.cast(TimestampType()).cast(LongType()) - y.cast(TimestampType()).cast(LongType())) % 3600 == 0, F.lit('hour shift'))
                    .when(F.date_trunc('millisecond', x.cast(TimestampType())) == F.date_trunc('millisecond', y.cast(TimestampType())), F.lit('millisecond truncation'))
                    .when(F.date_trunc('second', x.cast(TimestampType())) == y.cast(TimestampType()), F.lit('time removed'))
                    .when(F.date_trunc('second', y.cast(TimestampType())) == x.cast(TimestampType()), F.lit('time added'))
                    .otherwise(col_default)
                )
                .when(
                    data_type == F.lit('boolean'),
                    F.when(x.cast(BooleanType()) & ~y.cast(BooleanType()), "boolean flip (true -> false)")
                    .when(y.cast(BooleanType()) & ~x.cast(BooleanType()), "boolean flip (false -> true)")
                    .otherwise(col_default)
                )
                .otherwise(col_default)
            )
        )

    @cached_property
    def df_diff(self):
        return (
            self.df_regression
            .select(
                F.col("pk"),
                F.explode(F.col("diffs")).alias("diff"),
            )
            .select(
                F.col("diff.column_name").alias("column_name"),
                F.col("diff.data_type").alias("data_type"),
                F.col("pk"),
                self.__quote_if_string(F.col("diff.old_value"), F.col("diff.data_type")).alias("old_value"),
                self.__quote_if_string(F.col("diff.new_value"), F.col("diff.data_type")).alias("new_value"),
                self.__diff_category(
                    F.col("diff.old_value"),
                    F.col("diff.new_value"),
                    F.col("diff.data_type"),
                ).alias("diff_category"),
            )
        )

    @cached_property
    def df_diff_summary(self) -> DataFrame:
        "A summary of df_diff, aggregating a count of records/pks"
        return (
            self.df_diff
            .groupBy(F.col('column_name'), F.col('data_type'), F.col('diff_category'))
            .agg(F.count('pk').alias('count_record'), F.countDistinct('pk').alias('count_pk'))
            .orderBy(F.col('column_name'), F.col('diff_category'))
            .withColumn('count_pk_%oT', F.concat(F.round(F.col('count_pk') / F.lit(self.count_pk_comparable) * 100, 1).cast(StringType()), F.lit('%')))
        )

    # For each column_name and diff_category, provide samples
    @cached_property
    def df_diff_sample(self) -> DataFrame:
        "Same as df_diff, but limited to num_sample rows per column per diff category"
        return (
            self.df_diff
            .withColumn('rn', F.row_number().over(Window.partitionBy([F.col('column_name'), F.col('diff_category')]).orderBy(F.col('pk'))))
            .filter(F.col('rn') <= F.lit(self.num_samples))
            .drop('rn')
            .orderBy(F.col('column_name'), F.col('diff_category'), F.col('pk'))
        )
    # -------------------------------------------------------------------------

    # Regression Test Results
    # -------------------------------------------------------------------------
    @cached_property
    def success(self) -> bool:
        """
        Whether the table passed regression test.

        Regression Tests fail when:
        - Columns are added
        - Columns are removed
        - Columns change data_type
        - Duplicates exist (and are not symmetrical)
        - Orphans exist
        - Value Diffs exist
        """
        if self.columns_added:
            return False
        elif self.columns_removed:
            return False
        elif self.columns_changed_type:
            return False
        elif self.count_pk_duplicate_old > 0 and not self.has_symmetric_duplicates:
            return False
        elif self.count_pk_orphan_old > 0 or self.count_pk_orphan_new > 0:
            return False
        elif self.count_pk_diff > 0:
            return False
        else:
            return True
    # -------------------------------------------------------------------------

    # Reports
    # -------------------------------------------------------------------------
    def get_report(self) -> str:
        """
        A string-based report that summarizes the results of the Regression Test in Markdown.
        """
        timer_start = perf_counter()
        report: List[str] = list()

        if self.success:
            report.append(f"# {self.table_name}: SUCCESS")
        else:
            report.append(f"# {self.table_name}: FAILURE")

        report += [
            f"- run_id: {self.run_id}",
            f"- run_time: {str(self.run_time)}",
        ]

        if not self.success:
            report += [
                "\n### Table stats",
                f"- Count records in old {self.table_name}: {self.count_record_old}",
                f"- Count records in new {self.table_name}: {self.count_record_new}",
                f"- Count pks in old {self.table_name}: {self.count_pk_old}",
                f"- Count pks in new {self.table_name}: {self.count_pk_new}",
            ]

        if self.columns_added or self.columns_removed:
            report.append("\n### Column Changes")
            if self.columns_added:
                report.append(f"- Columns Added: {list(self.columns_added)}")
            if self.columns_removed:
                report.append(f"- Columns Removed: {list(self.columns_removed)}")

        if self.schema_mutations:
            report.append("\n### Schema Mutations")
            for sm in self.schema_mutations:
                report.append(f"- For column '{sm.column_name}', attribute '{sm.attribute}' changed from '{sm.value_old}' to '{sm.value_new}'.")

        if self.count_record_duplicate_old or self.count_record_duplicate_new:
            report += [
                "\n### Duplicates",
                f"- Count of duplicate records in old {self.table_name}: {self.count_record_duplicate_old}" + f" (%oT: {(self.count_record_duplicate_old / self.count_record_old):.1%})" if self.count_record_old > 0 else "", # noqa: E501
                f"- Count of duplicate records in new {self.table_name}: {self.count_record_duplicate_new}" + f" (%oT: {(self.count_record_duplicate_new / self.count_record_new):.1%})" if self.count_record_new > 0 else "", # noqa: E501
                f"- Count of duplicate pks in old {self.table_name}: {self.count_pk_duplicate_old}" + f" (%oT: {(self.count_pk_duplicate_old / self.count_pk_old):.1%})" if self.count_pk_old > 0 else "", # noqa: E501
                f"- Count of duplicate pks in new {self.table_name}: {self.count_pk_duplicate_new}" + f" (%oT: {(self.count_pk_duplicate_new / self.count_pk_new):.1%})" if self.count_pk_new > 0 else "", # noqa: E501
                f"- Sample of duplicate pks in old {self.table_name}: {[str(sample) for sample in self.sample_pk_duplicate_old]}",
                f"- Sample of duplicate pks in new {self.table_name}: {[str(sample) for sample in self.sample_pk_duplicate_new]}",
            ]
            if self.has_symmetric_duplicates:
                report.append("**NOTE: Duplicates are exactly the same between df_old and df_new**")

        if self.count_pk_orphan_old or self.count_pk_orphan_new:
            report += [
                "\n### Orphans",
                f"- Count of orphan pks in old {self.table_name}: {self.count_pk_orphan_old}" + f" (%oT: {(self.count_pk_orphan_old / self.count_pk_old):.1%})" if self.count_pk_old > 0 else "", # noqa: E501
                f"- Count of orphan pks in new {self.table_name}: {self.count_pk_orphan_new}" + f" (%oT: {(self.count_pk_orphan_new / self.count_pk_new):.1%})" if self.count_pk_new > 0 else "", # noqa: E501
                f"- Sample of orphan pks in old {self.table_name}: {[str(sample) for sample in self.sample_pk_orphan_old]}",
                f"- Sample of orphan pks in new {self.table_name}: {[str(sample) for sample in self.sample_pk_orphan_new]}",
            ]

        if self.count_pk_diff:
            report += [
                "\n### Diffs",
                f"- Columns with diffs: {self.columns_diff}",
                f"- Pks with diffs: {self.count_pk_diff} (%oT: {(self.count_pk_diff / self.count_pk_comparable):.1%})\n",
                "Diff Summary:\n",
                self.df_diff_summary.toPandas().to_markdown(index=False),
                "\n",
                "Diff Samples: (5 samples per column_name, per diff_category, per is_duplicate)\n",
                self.df_diff_sample.toPandas().to_markdown(index=False),
            ]

        timer_stop = perf_counter()
        time = str(timedelta(seconds=round(timer_stop - timer_start, 0)))
        time_split = time.split(':')
        report.append(f"\n> Time to complete: {time_split[0]} Hours {time_split[1]} Minutes {time_split[2]} Seconds\n")

        return "\n".join(report)
