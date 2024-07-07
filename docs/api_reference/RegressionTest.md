# RegressionTestResult

`RegressionTestResult` is a dataclass with methods for inspecting the difference between two dataframes.

::: pyspark_regression.RegressionTest
    handler: python
    options:
      members:
        - columns_old
        - columns_new
        - columns_all
        - columns_added
        - columns_removed
        - columns_kept
        - columns_changed_type
        - columns_changed_nullable
        - columns_changed_metadata
        - columns_comparable
        - columns_diff
        - schema_mutations
        - schema_mutations_type
        - schema_mutations_nullable
        - schema_mutations_metadata
        - count_record_old
        - count_record_new
        - count_pk_old
        - count_pk_new
        - count_duplicate_record_old
        - count_duplicate_record_new
        - count_duplicate_pk_old
        - count_duplicate_pk_new
        - has_symmetric_duplicates
        - count_orphan_pk_old
        - count_orphan_pk_new
        - count_comparable_record
        - count_comparable_pk
        - count_diff_record
        - count_diff_pk
        - sample_duplicate_pk_old
        - sample_duplicate_pk_new
        - sample_orphan_pk_old
        - sample_orphan_pk_new
        - df_duplicate_old
        - df_duplicate_new
        - df_orphan_old
        - df_orphan_new
        - df_comparable
        - df_regression
        - df_diff_cols
        - df_diff
        - df_diff_summary
        - df_diff_sample
        - is_success
        - summary
      show_root_heading: true
      show_source: true

