from bronze_connector import *
from bronze_lib import *
from silver_raw_ingest import *
from datetime import timedelta, timezone
from typing import Any, List, Mapping, Tuple


def read_data_from_source(
    spark: Any,
    source: str,
    df_meta_map_src_obj: Any,
    namespace: str,
    src_table_name: str,
    airconf: Mapping[str, Any],
    pull_method: str,
    src_target_table_name: str,
) -> Any:
    """Read source data for a single object using the configured pull strategy.

    Args:
        spark: Active Spark session used to execute the source read.
        source: Source system identifier used by the connector layer.
        df_meta_map_src_obj: Metadata dataframe for source-to-target object mapping.
        namespace: Source namespace, schema, or keyspace.
        src_table_name: Source table or object name to ingest.
        airconf: Runtime configuration values passed from orchestration.
        pull_method: Extraction mode for the source object.
        src_target_table_name: Canonical target object name derived from metadata.

    Returns:
        Source dataframe returned by the pull-based connector.
    """
    read_source_df = df_from_source_pull_based(
        spark,
        source,
        namespace,
        src_table_name,
        airconf,
        pull_method,
        df_meta_map_src_obj,
        src_target_table_name,
    )
    return read_source_df


def read_table(
    spark: Any,
    SNOWFLAKE_SOURCE_NAME: str,
    namespace: str,
    sfOptionsRaw: Any,
    processinggrp: str,
    awsbucket: str,
) -> Tuple[List[str], Any]:
    """Load source object metadata from the Snowflake configuration table.

    Args:
        spark: Active Spark session used to query metadata.
        SNOWFLAKE_SOURCE_NAME: Snowflake source identifier for the metadata lookup.
        namespace: Namespace whose objects should be selected.
        sfOptionsRaw: Snowflake connection options.
        processinggrp: Processing group to filter metadata rows.
        awsbucket: S3 bucket used by the ingestion process.

    Returns:
        A tuple containing the list of source table names to process and the
        metadata dataframe used to derive those names.
    """
    df_meta_map_src_tgt_obj = read_sf_meta_table(
        spark, SNOWFLAKE_SOURCE_NAME, namespace, sfOptionsRaw, processinggrp, awsbucket
    )

    table_list = get_list_table_meta_obj_sf(df_meta_map_src_tgt_obj)
    return table_list, df_meta_map_src_tgt_obj


def read_iceberg_config_table(
    spark: Any, namespace: str, processinggrp: str, icbg_config_db: str
) -> Tuple[List[str], Any]:
    """Load source object metadata from the Iceberg configuration table.

    Args:
        spark: Active Spark session used to query metadata.
        namespace: Namespace whose objects should be selected.
        processinggrp: Processing group to filter metadata rows.
        icbg_config_db: Iceberg database that stores bronze configuration.

    Returns:
        A tuple containing the list of source table names to process and the
        metadata dataframe used to derive those names.
    """
    df_meta_map_src_tgt_obj_iceberg = read_iceberg_meta_obj_table(
        spark, icbg_config_db, namespace, processinggrp
    )

    table_list = get_list_table_meta_obj_iceberg(df_meta_map_src_tgt_obj_iceberg)

    return table_list, df_meta_map_src_tgt_obj_iceberg


def bronze_work(
    spark: Any,
    awsbucket: str,
    namespace: str,
    processinggrp: str,
    noofoutfiles: int,
    sfOptionsRaw: Any,
    airconf: Mapping[str, Any],
    sfdatabaseraw: str,
    sfSchema: str,
    SNOWFLAKE_SOURCE_NAME: str,
) -> None:
    """Execute the bronze ingestion flow for all configured source objects.

    The workflow reads object metadata from the Snowflake table PIPELINE_CONFIG
    (or an Iceberg config table), pulls source data for each configured table, writes
    raw outputs to S3 or Iceberg targets, and updates downstream metadata.

    Args:
        spark: Active Spark session used for metadata and data operations.
        awsbucket: S3 bucket used for bronze output and checkpoint paths.
        namespace: Namespace whose configured source objects should be processed.
        processinggrp: Processing group to filter metadata rows.
        noofoutfiles: Number of output files to generate for S3 writes.
        sfOptionsRaw: Snowflake connection options.
        airconf: Runtime configuration values passed from orchestration.
        sfdatabaseraw: Snowflake raw database used for metadata updates.
        sfSchema: Snowflake schema used for metadata updates.
        SNOWFLAKE_SOURCE_NAME: Snowflake source identifier for audit writes.

    Raises:
        Exception: Re-raises any exception encountered while processing a table.
    """
    try:
        config_table_source = airconf.get("config_table_source", "snowflake")
        config_table_database = airconf.get("config_table_db", None)
        if config_table_source.lower() == "iceberg":
            print("Getting data from Iceberg PIPELINE_CONFIG")
            table_list, df_meta_map_src_tgt_obj = read_iceberg_config_table(
                spark, namespace, processinggrp, config_table_database
            )

            print("table_list", table_list)
        else:
            print("Getting data from Snowflake PIPELINE_CONFIG")
            table_list, df_meta_map_src_tgt_obj = read_table(
                spark,
                SNOWFLAKE_SOURCE_NAME,
                namespace,
                sfOptionsRaw,
                processinggrp,
                awsbucket,
            )

            print("table_list", table_list)

        if len(table_list) > 0:
            for table_name in table_list:
                print("BRONZE IS RUNNING FOR :", table_name)

                (
                    pull_method,
                    flattening_type,
                    src_processing_value,
                    target_table_name,
                    source,
                    src_target_table_name,
                    target_object,
                ) = get_meta_obj_field(
                    df_meta_map_src_tgt_obj, table_name, processinggrp
                )

                source_raw_data = read_data_from_source(
                    spark,
                    source,
                    df_meta_map_src_tgt_obj,
                    namespace,
                    table_name,
                    airconf,
                    pull_method,
                    src_target_table_name,
                )

                print("source_raw_data>>>>>")

                source_raw_data.show(n=10, truncate=False)

                source_data = hash_rename(source_raw_data)

                full_data_path, inc_s3_path, d = s3_path_creation(
                    awsbucket, src_target_table_name
                )

                if pull_method == "FULL_EXTRACT_INCR":
                    data_wth_hash = prepare_data_md5(source_data)
                    data_wth_hash = ingest_dt_in_df(data_wth_hash, d)

                else:
                    if target_object.lower() == "glue_iceberg_raw_table":
                        d = datetime.datetime.now()
                    source_data = ingest_dt_in_df(source_data, d)

                    data_wth_hash = source_data

                    data_wth_hash = prepare_data_md5(data_wth_hash)

                data_wth_hash_cached = data_wth_hash.cache()

                if pull_method == "FULL_EXTRACT_INCR":

                    exist = bucket_path_check(src_target_table_name, awsbucket)

                    if exist:
                        full_load_data_backup(awsbucket, src_target_table_name)

                        full_data_s3 = read_s3_data_full(spark, full_data_path)

                        # IMP : DO NOT REMOVE BELOW  cache() and SHOW() ACTION
                        full_data_s3_read = full_data_s3.cache()
                        full_data_s3_read.show(n=1, truncate=False)

                        inc_data_df = inc_data_check(
                            data_wth_hash_cached, full_data_s3_read
                        )
                        inc_data = inc_data_df.cache()

                        inc_data.count()
                        inc_write_s3(inc_data, inc_s3_path, noofoutfiles)
                        write_full_s3(
                            data_wth_hash_cached, full_data_path, noofoutfiles
                        )

                        prefix = prefix_for_bronze(awsbucket, inc_s3_path)
                        df_for_meta_log = meta_log_processing_df(
                            spark, prefix, awsbucket, src_target_table_name
                        )
                        write_df_sf_meta_log_process_false(
                            df_for_meta_log, SNOWFLAKE_SOURCE_NAME, sfOptionsRaw
                        )

                        if flattening_type == "SNOWFLAKE":
                            try:

                                list_of_fileid, path_list = get_list_parquet_files(
                                    spark,
                                    src_processing_value,
                                    awsbucket,
                                    table_name,
                                    pull_method,
                                )

                                s3_read_lander(
                                    spark,
                                    target_table_name,
                                    SNOWFLAKE_SOURCE_NAME,
                                    table_name,
                                    sfOptionsRaw,
                                    path_list,
                                )

                                meta_log_flag_update(
                                    spark,
                                    sfdatabaseraw,
                                    sfSchema,
                                    sfOptionsRaw,
                                    SNOWFLAKE_SOURCE_NAME,
                                    list_of_fileid,
                                )
                            except Exception as e:
                                logging.exception(
                                    "exception while writing into snowflake" + str(e)
                                )
                                raise e

                    else:

                        write_full_s3(
                            data_wth_hash_cached, full_data_path, noofoutfiles
                        )
                        inc_write_s3(data_wth_hash_cached, inc_s3_path, noofoutfiles)

                        prefix = prefix_for_bronze(awsbucket, inc_s3_path)
                        df_for_meta_log = meta_log_processing_df(
                            spark, prefix, awsbucket, src_target_table_name
                        )

                        write_df_sf_meta_log_process_false(
                            df_for_meta_log, SNOWFLAKE_SOURCE_NAME, sfOptionsRaw
                        )

                        if flattening_type == "SNOWFLAKE":
                            try:

                                data_wth_hash_cached.unpersist()
                                spark.catalog.clearCache()
                                list_of_fileid, path_list = get_list_parquet_files(
                                    spark,
                                    src_processing_value,
                                    awsbucket,
                                    table_name,
                                    pull_method,
                                )
                                s3_read_lander(
                                    spark,
                                    target_table_name,
                                    SNOWFLAKE_SOURCE_NAME,
                                    table_name,
                                    sfOptionsRaw,
                                    path_list,
                                )

                                meta_log_flag_update(
                                    spark,
                                    sfdatabaseraw,
                                    sfSchema,
                                    sfOptionsRaw,
                                    SNOWFLAKE_SOURCE_NAME,
                                    list_of_fileid,
                                )
                            except Exception as e:

                                logging.exception(
                                    "exception: writing into snowflake" + str(e)
                                )
                                raise e

                if pull_method == "FULL_EXTRACT":
                    d = datetime.datetime.now()
                    year = f"{d.year}"
                    month = f"{d.month:02d}"
                    day = f"{d.day:02d}"
                    hour = f"{d.hour:02d}"

                    full_data_path_s3 = (
                        full_data_path
                        + str(year)
                        + "/"
                        + str(month)
                        + "/"
                        + str(day)
                        + "/"
                        + str(hour)
                        + "/"
                    )

                    # Please note:
                    # target_object == 'Glue_Iceberg_Parquet_File' is for Dsync in NRT
                    # target_object.lower() == 'glue_iceberg_raw_table' is for loading data to raw iceberg table

                    if target_object == "Glue_Iceberg_Parquet_File":
                        write_full_s3_iceberg(
                            spark, source_data, src_target_table_name, awsbucket, source
                        )
                    elif target_object.lower() == "glue_iceberg_raw_table":
                        write_full_iceberg_table(
                            spark, source_data, src_target_table_name, awsbucket
                        )
                    else:
                        print("writing full data into s3")
                        write_full_s3(source_data, full_data_path_s3, noofoutfiles)

                        prefix = prefix_for_bronze(awsbucket, full_data_path_s3)
                        df_for_meta_log = meta_log_processing_df(
                            spark, prefix, awsbucket, src_target_table_name
                        )

                        write_df_sf_meta_log_process_false(
                            df_for_meta_log, SNOWFLAKE_SOURCE_NAME, sfOptionsRaw
                        )

                        if flattening_type == "SNOWFLAKE":
                            list_of_fileid, path_list = get_list_parquet_files(
                                spark,
                                src_processing_value,
                                awsbucket,
                                table_name,
                                pull_method,
                            )

                            s3_read_lander(
                                spark,
                                target_table_name,
                                SNOWFLAKE_SOURCE_NAME,
                                table_name,
                                sfOptionsRaw,
                                path_list,
                            )

                            meta_log_flag_update(
                                spark,
                                sfdatabaseraw,
                                sfSchema,
                                sfOptionsRaw,
                                SNOWFLAKE_SOURCE_NAME,
                                list_of_fileid,
                            )

                if pull_method == "KEY_INCR_EXTRACT":
                    s3_path_key_based = s3_path_for_key_based(
                        awsbucket, src_target_table_name
                    )
                    is_empty_flag = len(data_wth_hash_cached.head(1))
                    if target_object.lower() == "glue_iceberg_raw_table":
                        trgt_table_name = src_target_table_name
                        key_inc_iceberg_write(
                            spark, data_wth_hash_cached, trgt_table_name, awsbucket
                        )
                        fld_name = get_incrmt_fld(
                            df_meta_map_src_tgt_obj, trgt_table_name
                        )
                        src_to_fld_update = src_obj_to_fld_update_val_icbg(
                            data_wth_hash_cached, fld_name, source
                        )
                    else:
                        fld_name, _, _, fld_val = push_down_query(
                            df_meta_map_src_tgt_obj, src_target_table_name
                        )
                        key_inc_s3_write(
                            data_wth_hash_cached, s3_path_key_based, noofoutfiles
                        )
                        src_to_fld_update = src_obj_to_fld_update_val(
                            data_wth_hash_cached, fld_name, source
                        )
                    src_to_fld_update = last_str_trim(src_to_fld_update)
                    print("src_to_fld_update", src_to_fld_update)

                    if config_table_source.lower() == "iceberg":
                        trgt_table_name = src_target_table_name
                        src_to_fld_update_icbg(
                            spark,
                            src_to_fld_update,
                            is_empty_flag,
                            config_table_database,
                            namespace,
                            table_name,
                            trgt_table_name,
                            awsbucket,
                        )
                    else:
                        src_to_fld_update_sf(
                            spark,
                            src_to_fld_update,
                            sfdatabaseraw,
                            sfOptionsRaw,
                            sfSchema,
                            src_target_table_name,
                            namespace,
                            is_empty_flag,
                        )

                        prefix = prefix_for_bronze(awsbucket, s3_path_key_based)
                        df_for_meta_log = meta_log_processing_df(
                            spark, prefix, awsbucket, src_target_table_name
                        )

                        write_df_sf_meta_log_process_false(
                            df_for_meta_log, SNOWFLAKE_SOURCE_NAME, sfOptionsRaw
                        )
                print("Bronze execution is complete for: ", table_name)
                print("*" * 180)

    except Exception as e:
        logging.exception("Bronze ingestion did not run successfully :" + str(e))
        raise e
