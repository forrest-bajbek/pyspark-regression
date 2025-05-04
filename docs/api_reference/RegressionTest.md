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
        - count_record_duplicate_old
        - count_record_duplicate_new
        - count_pk_duplicate_old
        - count_pk_duplicate_new
        - has_symmetric_duplicates
        - count_pk_orphan_old
        - count_pk_orphan_new
        - count_pk_comparable
        - count_record_diff
        - count_pk_diff
        - sample_pk_duplicate_old
        - sample_pk_duplicate_new
        - sample_pk_orphan_old
        - sample_pk_orphan_new
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
        - success
        - summary
      show_root_heading: true
      show_source: true
