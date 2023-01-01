from datetime import datetime
from time import perf_counter
from uuid import UUID, uuid4
from typing import List, Tuple, Set
from dataclasses import dataclass
from tabulate import tabulate
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, LongType, StructType, StructField


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

    df_old: DataFrame
    "Old DataFrame"
    df_new: DataFrame
    "New DataFrame"
    pk: str
    "Primary Key shared between df_old and df_new. Can be either a string or a Spark Column."
    table_name: str = "df"
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

        if "pk" in self.df_old.columns:
            raise KeyError(
                "The column name 'pk' itself is reserved. Please remove it from df_old."
            )
        if "pk" in self.df_new.columns:
            raise KeyError(
                "The column name 'pk' itself is reserved. Please remove it from df_new."
            )

        spark.sparkContext.setCheckpointDir(f"/tmp/run_{self.run_id}/spark_checkpoint")

        # Add pk to df_old and df_new
        object.__setattr__(self, "df_old", self.df_old.withColumn("pk", F.col(self.pk)))
        object.__setattr__(self, "df_new", self.df_new.withColumn("pk", F.col(self.pk)))

    # Schema Analysis
    # -------------------------------------------------------------------------
    @property
    def columns_old(self) -> Set[str]:
        "Columns in df_old"
        return set([col for col in self.df_old.columns if not col == "pk"])

    @property
    def columns_new(self) -> Set[str]:
        "Columns in df_new"
        return set([col for col in self.df_new.columns if not col == "pk"])

    @property
    def columns_all(self) -> Set[str]:
        "All columns between df_old and df_new"
        return self.columns_new.union(self.columns_old)

    @property
    def columns_added(self) -> Set[str]:
        "Columns in df_new that are not in df_old"
        return self.columns_new.difference(self.columns_old)

    @property
    def columns_removed(self) -> Set[str]:
        "Columns in df_old that are not in df_new"
        return self.columns_old.difference(self.columns_new)

    @property
    def columns_kept(self) -> Set[str]:
        "Columns that are in both df_old and df_new"
        return self.columns_old.intersection(self.columns_new)

    @property
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

    @property
    def schema_mutations_type(self) -> Set[str]:
        "Detail for schema mutations of 'data_type' attribute"
        return set([sm for sm in self.schema_mutations if sm.attribute == "type"])

    @property
    def schema_mutations_nullable(self) -> Set[str]:
        "Detail for schema mutations of 'nullable' attribute"
        return set([sm for sm in self.schema_mutations if sm.attribute == "nullable"])

    @property
    def schema_mutations_metadata(self) -> Set[str]:
        "Detail for schema mutations of 'metadata' attribute"
        return set([sm for sm in self.schema_mutations if sm.attribute == "metadata"])

    @property
    def columns_changed_type(self) -> Set[str]:
        "Columns in both df_old and df_new for which their 'data_type' attribute changed"
        return set([sm.column_name for sm in self.schema_mutations_type])

    @property
    def columns_changed_nullable(self) -> Set[str]:
        "Columns in both df_old and df_new for which their 'nullable' attribute changed"
        return set([sm.column_name for sm in self.schema_mutations_nullable])

    @property
    def columns_changed_metadata(self) -> Set[str]:
        "Columns in both df_old and df_new for which their 'metadata' attribute changed"
        return set([sm.column_name for sm in self.schema_mutations_metadata])

    @property
    def columns_comparable(self) -> Set[str]:
        """
        Columns that can be compared for regression test. Comparable columns (1) must be
        present in df_old and df_new, and (2) have the same data_type in df_old and df_new
        """
        return self.columns_kept.difference(self.columns_changed_type)

    # -------------------------------------------------------------------------

    # Base Table Info
    # -------------------------------------------------------------------------
    @property
    def count_record_old(self) -> int:
        "Count of records in df_old"
        return self.df_old.count()

    @property
    def count_record_new(self) -> int:
        "Count of records in df_new"
        return self.df_new.count()

    @property
    def count_pk_old(self) -> int:
        "Count of pks in df_old"
        return self.df_old.select(F.col("pk")).distinct().count()

    @property
    def count_pk_new(self) -> int:
        "Count of pks in df_new"
        return self.df_new.select(F.col("pk")).distinct().count()

    # -------------------------------------------------------------------------

    # Duplicate Analysis
    # -------------------------------------------------------------------------
    @staticmethod
    def __df_duplicate(df: DataFrame) -> DataFrame:
        return (
            df.select(F.col("pk"))
            .groupBy(F.col("pk"))
            .agg((F.count(F.col("pk")) - 1).alias("count_record_duplicate"))
            .filter(F.col("count_record_duplicate") > 0)
            .orderBy(
                [F.col("count_record_duplicate").desc(), F.col("pk")]
            )  # sort worst offenders first, then pk
        )

    @property
    def df_duplicate_old(self) -> DataFrame:
        "DataFrame of duplicate pks in old table, with count of duplicates"
        return self.__df_duplicate(self.df_old)

    @property
    def df_duplicate_new(self) -> DataFrame:
        "DataFrame of duplicate pks in new table, with count of duplicates"
        return self.__df_duplicate(self.df_new)

    @staticmethod
    def __count_record_duplicate(df_duplicate: DataFrame) -> int:
        return [
            row.crd
            for row in df_duplicate.agg(
                F.sum(F.col("count_record_duplicate")).alias("crd")
            ).collect()
        ][0] or 0

    @property
    def count_record_duplicate_old(self) -> int:
        "Count of duplicate records in df_old"
        return self.__count_record_duplicate(self.df_duplicate_old)

    @property
    def count_record_duplicate_new(self) -> int:
        "Count of duplicate records in df_new"
        return self.__count_record_duplicate(self.df_duplicate_new)

    @property
    def count_pk_duplicate_old(self) -> int:
        "Count of pks that have duplicates in df_old"
        return self.df_duplicate_old.count() or 0

    @property
    def count_pk_duplicate_new(self) -> int:
        "Count of pks that have duplicates in df_new"
        return self.df_duplicate_new.count() or 0

    @staticmethod
    def __sample_pk_duplicate(df_duplicate: DataFrame, num_samples: int) -> tuple:
        return tuple(
            [
                row.pk
                for row in df_duplicate.select(F.col("pk")).limit(num_samples).collect()
            ]
        )

    @property
    def sample_pk_duplicate_old(self) -> tuple:
        "num_sample samples of pks that have duplicate records in df_old"
        return self.__sample_pk_duplicate(
            self.df_duplicate_old, num_samples=self.num_samples
        )

    @property
    def sample_pk_duplicate_new(self) -> tuple:
        "num_sample samples of pks that have duplicate records in df_new"
        return self.__sample_pk_duplicate(
            self.df_duplicate_new, num_samples=self.num_samples
        )

    @property
    def has_symmetric_duplicates(self) -> bool:
        "True if duplicates in df_old and df_new are exactly the same."
        if (
            self.count_record_duplicate_old > 0
        ):  # Can't have symmetric duplicates without at least one duplicate
            __df_dup_old_comp = self.df_old.join(
                self.df_duplicate_old, how="left_semi", on=["pk"]
            ).select(list(self.columns_comparable))
            __df_dup_new_comp = self.df_new.join(
                self.df_duplicate_new, how="left_semi", on=["pk"]
            ).select(list(self.columns_comparable))
            return (__df_dup_old_comp.exceptAll(__df_dup_new_comp)).unionAll(
                __df_dup_new_comp.exceptAll(__df_dup_old_comp)
            ).count() == 0
        else:
            return False

    # -------------------------------------------------------------------------

    # Orphan Analysis
    # -------------------------------------------------------------------------
    @staticmethod
    def __df_orphan(df: DataFrame, df_anti: DataFrame) -> DataFrame:
        return (
            df.join(df_anti, how="left_anti", on=["pk"])
            .select(F.col("pk"))
            .distinct()
            .orderBy(F.col("pk"))
        )

    @property
    def df_orphan_old(self) -> DataFrame:
        "DataFrame of orphan pks in old table"
        return self.__df_orphan(df=self.df_old, df_anti=self.df_new)

    @property
    def df_orphan_new(self) -> DataFrame:
        "DataFrame of orphan pks in new table"
        return self.__df_orphan(df=self.df_new, df_anti=self.df_old)

    @property
    def count_pk_orphan_old(self) -> int:
        "Count of pks that are in df_old but not df_new"
        return self.df_orphan_old.count() or 0

    @property
    def count_pk_orphan_new(self) -> int:
        "Count of pks that are in df_new but not df_old"
        return self.df_orphan_new.count() or 0

    @property
    def sample_pk_orphan_old(self) -> tuple:
        "num_sample samples of pks that are in df_old but not df_new"
        return tuple(
            [
                row.pk
                for row in self.df_orphan_old.select(F.col("pk"))
                .limit(self.num_samples)
                .collect()
            ]
        )

    @property
    def sample_pk_orphan_new(self) -> tuple:
        "num_sample samples of pks that are in df_new but not df_old"
        return tuple(
            [
                row.pk
                for row in self.df_orphan_new.select(F.col("pk"))
                .limit(self.num_samples)
                .collect()
            ]
        )

    # -------------------------------------------------------------------------

    # Value Analysis
    # -------------------------------------------------------------------------
    @staticmethod
    def __generate_regression_sql(columns_comparable: set) -> Tuple[str, str]:
        """
        Generates the CASE and WHERE statements needed to compare the values between two dataframes.

        Returns: (case_sql, where_sql)
        """
        case_sql = list()
        where_sql = list()
        for i, col in enumerate(columns_comparable):
            o = col + "_old"
            n = col + "_new"
            case_sql.append(
                f"CASE WHEN (({o} != {n}) OR ({o} IS NULL AND {n} IS NOT NULL) OR ({o} IS NOT NULL AND {n} IS NULL)) THEN '{col}' ELSE NULL END"
            )
            where_sql.append(
                f"(({o} != {n}) OR ({o} IS NULL AND {n} IS NOT NULL) OR ({o} IS NOT NULL AND {n} IS NULL))"
            )

        case_sql = f"SPLIT(CONCAT_WS(',', {','.join(case_sql)}), ',')"
        where_sql = " OR ".join(where_sql)

        return case_sql, where_sql

    @staticmethod
    def __rename_cols(
        df: DataFrame, *, prefix: str = None, suffix: str = None
    ) -> DataFrame:
        """
        Renames every column in df using prefix (if defined) and suffix (if defined)

        Args:
            df (DataFrame): Dataframe with columns you want to rename.
            prefix (str = None): Text to append before the column name.
            suffix (str = None): Text to append after the column name.
        Returns:
            DataFrame with columns renamed using prefix and suffix
        """
        _p = prefix + "_" if prefix else ""
        _s = "_" + suffix if suffix else ""
        new_cols = list()
        for col in df.columns:
            p = "" if col == "pk" else _p
            s = "" if col == "pk" else _s
            new_cols.append(f"{p}{col}{s}")
        return df.toDF(*(new_cols))

    @property
    def df_comparable(self) -> DataFrame:
        # remove duplicates. they cross join each other
        df_old_no_dups = self.df_old.join(
            self.df_duplicate_old, how="left_anti", on=["pk"]
        ).select(list(self.columns_comparable) + ["pk"])
        df_new_no_dups = self.df_new.join(
            self.df_duplicate_new, how="left_anti", on=["pk"]
        ).select(list(self.columns_comparable) + ["pk"])

        df_old_renamed = self.__rename_cols(df_old_no_dups, suffix="old")
        df_new_renamed = self.__rename_cols(df_new_no_dups, suffix="new")

        df_comparable = df_old_renamed.join(df_new_renamed, on=["pk"])

        return df_comparable

    @property
    def count_record_comparable(self) -> int:
        "Count of comparable records between df_old and df_new"
        return self.df_comparable.count()

    @property
    def count_pk_comparable(self) -> int:
        "Count of comparable pks between df_old and df_new"
        return self.df_comparable.select(F.col("pk")).distinct().count()

    @property
    def df_regression(self) -> DataFrame:
        case_sql, where_sql = self.__generate_regression_sql(
            columns_comparable=self.columns_comparable
        )
        df_regression = self.df_comparable.filter(F.expr(where_sql)).withColumn(
            "diff_cols", F.expr(case_sql)
        )
        return df_regression

    @property
    def count_record_diff(self) -> int:
        "Count of records with diffs"
        return self.df_regression.count() or 0

    @property
    def count_pk_diff(self) -> int:
        "Count of pks with diffs"
        return self.df_regression.select(F.col("pk")).distinct().count() or 0

    @property
    def df_diff_cols(self) -> DataFrame:
        return (
            self.df_regression.select(F.explode(F.col("diff_cols")).alias("diff_cols"))
            .distinct()
            .orderBy(F.col("diff_cols"))
        )

    @property
    def columns_diff(self) -> Set[str]:
        "Columns containing at least one value difference between df_old and df_new"
        return set([row.diff_cols for row in self.df_diff_cols.collect()])

    # -------------------------------------------------------------------------

    # Regression Test Results
    # -------------------------------------------------------------------------
    @property
    def success(self) -> bool:
        """
        Whether the table passed regression test.

        Regression Tests fail when:
        - Columns are removed
        - Columns change data_type
        - Duplicates exist (and are not symmetrical)
        - Orphans exist
        - Value Diffs exist
        """
        if self.columns_removed:
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

    # Diff Analysis
    # -------------------------------------------------------------------------
    @staticmethod
    def __categorize_diff(x: Column, y: Column, *, data_type: str) -> Column:
        """
        Categorizes the difference between two columns.
        """
        col_default = F.lit("uncategorized")

        if data_type == "string":
            col_diff_category = (
                F.when(
                    (F.trim(y) == x) & (F.ltrim(y) != x) & (F.rtrim(y) != x),
                    F.lit("padding added (left and right)"),
                )
                .when(
                    (F.trim(x) == y) & (F.ltrim(x) != y) & (F.rtrim(x) != y),
                    F.lit("padding removed (left and right)"),
                )
                .when(F.ltrim(y) == x, F.lit("padding added (left)"))
                .when(F.rtrim(y) == x, F.lit("padding added (right)"))
                .when(F.ltrim(x) == y, F.lit("padding removed (left)"))
                .when(F.rtrim(x) == y, F.lit("padding removed (right)"))
                .when(F.upper(x) == y, F.lit("capitalization added"))
                .when(F.lower(y) == x, F.lit("capitalization added"))
                .when(F.upper(y) == x, F.lit("capitalization removed"))
                .when(F.lower(x) == y, F.lit("capitalization removed"))
                .when(F.upper(x) == F.upper(y), F.lit("capitalization changed"))
                .when(x.startswith(y), F.lit("truncation added"))
                .when(y.startswith(x), F.lit("truncation removed"))
                .otherwise(col_default)
            )
        elif data_type in ["float", "double", "decimal"]:
            col_diff_category = F.when(
                F.round(x, 2) == F.round(y, 2), F.lit("rounding")
            ).otherwise(col_default)
        elif data_type == "timestamp":
            col_diff_category = (
                F.when(
                    F.abs(x.cast(LongType()) - y.cast(LongType())) % 3600 == 0,
                    F.lit("hour shift"),
                )
                .when(
                    F.date_trunc("millisecond", x) == F.date_trunc("millisecond", y),
                    F.lit("millisecond truncation"),
                )
                .otherwise(col_default)
            )
        elif data_type == "boolean":
            col_diff_category = (
                F.when(x & ~y, "boolean flip (true -> false)")
                .when(y & ~x, "boolean flip (false -> true)")
                .otherwise(col_default)
            )
        else:
            col_diff_category = col_default

        return (
            F.when(x.isNull() & y.isNotNull(), F.lit("null flip (null -> not null)"))
            .when(x.isNotNull() & y.isNull(), F.lit("null flip (not null -> null)"))
            .otherwise(col_diff_category)
        )

    @property
    def df_diff(self) -> DataFrame:
        "DataFrame containing an unpivoted representation of diffs for a pk,  in a single column, between df_old and df_new"

        schema = StructType(
            [
                StructField(name="column_name", dataType=StringType(), nullable=False),
                StructField(name="data_type", dataType=StringType(), nullable=False),
                StructField(name="pk", dataType=StringType(), nullable=False),
                StructField(name="old_value", dataType=StringType()),
                StructField(name="new_value", dataType=StringType()),
                StructField(
                    name="diff_category", dataType=StringType(), nullable=False
                ),
            ]
        )

        df_diff = spark.createDataFrame(data=[], schema=schema)
        quote_if_string = (
            lambda col, data_type: F.concat(F.lit("'"), col, F.lit("'"))
            if data_type == "string"
            else col
        )
        for col in self.columns_diff:
            data_type = self.df_old.schema[col].dataType.typeName()
            df = (
                self.df_regression.filter(
                    F.array_contains(F.col("diff_cols"), F.lit(col))
                )
                .select(
                    F.lit(col).alias("column_name"),
                    F.lit(data_type).alias("data_type"),
                    F.col("pk").alias("pk"),
                    quote_if_string(
                        F.col(f"{col}_old").cast(StringType()), data_type=data_type
                    ).alias("old_value"),
                    quote_if_string(
                        F.col(f"{col}_new").cast(StringType()), data_type=data_type
                    ).alias("new_value"),
                    self.__categorize_diff(
                        F.col(f"{col}_old"),
                        F.col(f"{col}_new"),
                        data_type=data_type,
                    ).alias("diff_category"),
                )
                .distinct()
            )
            df_diff = df_diff.unionAll(df)

        return df_diff.orderBy(
            F.col("column_name"), F.col("diff_category"), F.col("pk")
        )

    @property
    def df_diff_summary(self) -> DataFrame:
        "A summary of df_diff, aggregating a count of records/pks"
        return (
            self.df_diff.groupBy(
                F.col("column_name"), F.col("data_type"), F.col("diff_category")
            )
            .agg(
                F.count("pk").alias("count_record"),
            )
            .orderBy(F.col("column_name"), F.col("diff_category"))
            .withColumn(
                "count_record_%oT",
                F.concat(
                    F.round(
                        F.col("count_record")
                        / F.lit(self.count_record_comparable)
                        * 100,
                        1,
                    ).cast(StringType()),
                    F.lit("%"),
                ),
            )
        )

    # For each column_name and diff_category, provide samples
    @property
    def df_diff_sample(self) -> DataFrame:
        "Same as df_diff, but limited to num_sample rows per column per diff category"
        return (
            self.df_diff.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy(
                        [F.col("column_name"), F.col("diff_category")]
                    ).orderBy(F.col("pk"))
                ),
            )
            .filter(F.col("rn") <= F.lit(self.num_samples))
            .drop("rn")
            .orderBy(F.col("column_name"), F.col("diff_category"), F.col("pk"))
        )

    # -------------------------------------------------------------------------

    # Reports
    # -------------------------------------------------------------------------
    @property
    def report(self) -> str:
        """
        A string-based report that summarizes the results of the Regression Test in Markdown. Intended for the Quality Notebook.
        """
        report = str()

        report += f"\n# Regression Test: {self.table_name}"
        report += f"\n- run_id: {self.run_id}"
        report += f"\n- run_time: {str(self.run_time)}"

        if self.success:
            report += "\n\n## Test Results: SUCCESS"
        else:
            report += "\n\n## Test Results: **FAILURE**."
            report += "\nPrinting Regression Report..."

            report += "\n\n### Table stats"
            report += (
                f"\n- Count records in old {self.table_name}: {self.count_record_old}"
            )
            report += (
                f"\n- Count records in new {self.table_name}: {self.count_record_new}"
            )
            report += f"\n- Count pks in old {self.table_name}: {self.count_pk_old}"
            report += f"\n- Count pks in new {self.table_name}: {self.count_pk_new}"

        if self.columns_added or self.columns_removed:
            report += f"\n\n### Column Changes"
            report += f"\n- Columns Added: {list(self.columns_added)}"
            if self.columns_removed:
                report += f"\n- Columns Removed: {list(self.columns_removed)}"

        if self.schema_mutations:
            report += f"\n\n### Schema Mutations"
            for sm in self.schema_mutations:
                report += f"\n- For column '{sm.column_name}', attribute '{sm.attribute}' changed from '{sm.value_old}' to '{sm.value_new}'."

        if self.count_record_duplicate_old or self.count_record_duplicate_new:
            report += f"\n\n### Duplicates"
            report += f"\n- Count of duplicate records in old {self.table_name}: {self.count_record_duplicate_old} (%oT: {(self.count_record_duplicate_old / self.count_record_old):.1%})"
            report += f"\n- Count of duplicate records in new {self.table_name}: {self.count_record_duplicate_new} (%oT: {(self.count_record_duplicate_new / self.count_record_new):.1%})"
            report += f"\n- Count of duplicate pks in old {self.table_name}: {self.count_pk_duplicate_old} (%oT: {(self.count_pk_duplicate_old / self.count_pk_old):.1%})"
            report += f"\n- Count of duplicate pks in new {self.table_name}: {self.count_pk_duplicate_new} (%oT: {(self.count_pk_duplicate_new / self.count_pk_new):.1%})"
            report += f"\n- Sample of duplicate pks in old {self.table_name}: {[str(sample) for sample in self.sample_pk_duplicate_old]}"
            report += f"\n- Sample of duplicate pks in new {self.table_name}: {[str(sample) for sample in self.sample_pk_duplicate_new]}"
            if self.has_symmetric_duplicates:
                report += "\n**NOTE: Duplicates are exactly the same between df_old and df_new**"

        if self.count_pk_orphan_old or self.count_pk_orphan_new:
            report += f"\n\n### Orphans"
            report += f"\n- Count of orphan pks in old {self.table_name}: {self.count_pk_orphan_old} (%oT: {(self.count_pk_orphan_old / self.count_pk_old):.1%})"
            report += f"\n- Count of orphan pks in new {self.table_name}: {self.count_pk_orphan_new} (%oT: {(self.count_pk_orphan_new / self.count_pk_new):.1%})"
            report += f"\n- Sample of orphan pks in old {self.table_name}: {[str(sample) for sample in self.sample_pk_orphan_old]}"
            report += f"\n- Sample of orphan pks in new {self.table_name}: {[str(sample) for sample in self.sample_pk_orphan_new]}"

        if self.count_record_diff:
            report += f"\n\n### Diffs"
            report += f"\n- Columns with diffs: {self.columns_diff}"
            report += f"\n- Number of records with diffs: {self.count_record_diff} (%oT: {(self.count_record_diff / self.count_record_comparable):.1%})"
            report += f"\n\n Diff Summary:\n"
            report += tabulate(
                self.df_diff_summary.toPandas(),
                headers="keys",
                missingval="NULL",
                tablefmt="pipe",
                showindex=False,
            )
            report += f"\n\n Diff Samples: (5 samples per column_name, per diff_category, per is_duplicate)\n"
            report += tabulate(
                self.df_diff_sample.toPandas(),
                headers="keys",
                missingval="NULL",
                tablefmt="pipe",
                showindex=False,
            )

        return report

    @property
    def change_log(self):
        pass

    # -------------------------------------------------------------------------
