import boto3
import datetime
import json
import logging
import re
import pyspark.sql.functions as f
from pyspark.sql.functions import col, lit
import pandas as pd
from datetime import timedelta, timezone
from typing import Any, List, Mapping, Optional, Sequence, Tuple


def s3_path_creation(aws_bucket: str, table: str) -> Tuple[str, str, datetime.datetime]:
    """Build the standard full-load and incremental S3 paths for a source table.

    Args:
        aws_bucket: Target S3 bucket name.
        table: Source table name.

    Returns:
        A tuple of full-load path, incremental path, and the rounded ingest timestamp.
    """
    full_data_path = "s3a://" + aws_bucket + "/" + table + "/" + "full_load/"

    d = datetime.datetime.now()
    year = f"{d.year}"
    month = f"{d.month:02d}"
    day = f"{d.day:02d}"
    hour = f"{d.hour:02d}"

    dt_ingest = datetime.datetime.now(timezone.utc)
    c = dt_ingest.strftime("%Y-%m-%d-%H:00")
    f = datetime.datetime.strptime(c, "%Y-%m-%d-%H:00")

    inc_s3_path = (
        "s3a://"
        + aws_bucket
        + "/"
        + table
        + "/"
        + "incr_load"
        + "/"
        + str(year)
        + "/"
        + str(month)
        + "/"
        + str(day)
        + "/"
        + str(hour)
        + "/"
    )

    return full_data_path, inc_s3_path, f


def bucket_path_check(table: str, aws_bucket: str) -> bool:
    """Check whether the full-load prefix already exists in S3.

    Args:
        table: Source table name.
        aws_bucket: Target S3 bucket name.

    Returns:
        `True` when the full-load prefix exists, otherwise `False`.
    """
    try:
        client = boto3.client("s3")
        prefix = table + "/" + "full_load/"
        bucket = aws_bucket
        result = client.list_objects(Bucket=bucket, Prefix=prefix)
        exist = False
        if "Contents" in result:
            exist = True
        return exist

    except Exception as e:
        logging.exception("exception during s3 path check module  >>>>>:" + str(e))
        raise e


def icbg_revert_to_previous_snapshot(
    spark: Any, icbg_db: str, snap_revert_table: str
) -> None:
    """Rollback an Iceberg table to its immediate parent snapshot.

    Args:
        spark: Active Spark session.
        icbg_db: Iceberg database name.
        snap_revert_table: Iceberg table to rollback.
    """

    # Getting current snapshot record
    current_snap_rec = spark.sql(f"""
    select * from my_icbg_catalog.{icbg_db}.{snap_revert_table}.snapshots
    where committed_at = 
    (select max(committed_at) from my_icbg_catalog.{icbg_db}.{snap_revert_table}.snapshots)
    """)
    print(
        f"Showing current snapshot_id record from {icbg_db}.{snap_revert_table}.snapshots"
    )
    current_snap_rec.show(5, truncate=False)

    parent_snapshot_id = current_snap_rec.select("parent_id").collect()[0][0]
    print(f"Reverting to parent_id snapshot: {parent_snapshot_id}")

    query_to_revert_snapshot = f"""
    CALL my_icbg_catalog.system.rollback_to_snapshot('my_icbg_catalog.{icbg_db}.{snap_revert_table}', 
    {parent_snapshot_id})
    """
    print("Snapshot Revert Query:", query_to_revert_snapshot)

    spark.sql(query_to_revert_snapshot).show(5, truncate=False)


def prepare_json_data(data: Any) -> Any:
    """Serialize each source row into a single JSON column.

    Args:
        data: Source dataframe.

    Returns:
        A dataframe with the serialized `SRC` column.
    """

    try:
        df_wth_json_col = data.withColumn(
            "SRC", f.to_json(f.struct([data[x] for x in data.columns]))
        )

        return df_wth_json_col

    except Exception as e:
        logging.exception("exception in md5 module >>>>>:" + str(e))
        raise e


def prepare_data_md5(data: Any) -> Any:
    """Add a deterministic row hash used for change detection.

    Args:
        data: Source dataframe.

    Returns:
        The dataframe with a `hash` column derived from row JSON.
    """

    df_wth_json_col = data.withColumn(
        "json_data", f.to_json(f.struct([data[x] for x in data.columns]))
    )

    return df_wth_json_col.withColumn("hash", f.md5("json_data")).drop("json_data")


def read_s3_data_full(spark: Any, full_path: str) -> Any:
    """Read the stored full-load parquet dataset from S3.

    Args:
        spark: Active Spark session.
        full_path: Full-load S3 parquet path.

    Returns:
        A Spark dataframe loaded from the supplied path.
    """
    try:
        s3_full_df = spark.read.format("parquet").load(full_path)
        return s3_full_df
    except Exception as e:
        print("printing full path", full_path)
        logging.exception(
            "exception while reading full data from s3 full_load bucket" + str(e)
        )
        raise e


def inc_data_check(cass_df_full: Any, s3_full_df: Any) -> Any:
    """Compute incremental rows by excluding hashes already present in full-load data.

    Args:
        cass_df_full: Current source dataframe.
        s3_full_df: Existing full-load dataframe.

    Returns:
        A dataframe containing only new rows.
    """
    try:
        # cass_df_full.cache()
        s3_full_df_hash = s3_full_df.select("hash")

        print("incremental data check module")
        s3_full_df_hash.show(n=1, truncate=False)

        inc_data_sf = cass_df_full.join(s3_full_df_hash, on="hash", how="left_anti")

        return inc_data_sf

    except Exception as e:

        logging.exception("exception in  inc check module >>>>>:" + str(e))
        raise e


def inc_write_s3(inc_data_s3: Any, inc_path: str, no_of_out_files: int) -> None:
    """Write incremental parquet output to S3.

    Args:
        inc_data_s3: Incremental dataframe to persist.
        inc_path: Target S3 parquet path.
        no_of_out_files: Number of output files to emit.
    """
    try:

        inc_data_s3 = inc_data_s3.coalesce(no_of_out_files)
        print("inc_path", inc_path)
        inc_data_s3.write.parquet(inc_path)

    except Exception as e:
        logging.exception(
            "exception during writing incremental data into s3 inc bucket" + str(e)
        )
        raise e


def write_full_s3(full_s3_data: Any, full_path: str, no_of_out_files: int) -> None:
    """Write a full-load parquet snapshot to S3.

    Args:
        full_s3_data: Dataframe to persist.
        full_path: Target S3 parquet path.
        no_of_out_files: Number of output files to emit.
    """
    try:

        full_s3_data = full_s3_data.coalesce(no_of_out_files)
        print("full_path", full_path)
        full_s3_data.write.parquet(full_path, mode="overwrite")
    except Exception as e:
        logging.exception(
            "exception while writing  full data into full_load bucket " + str(e)
        )
        raise e


def write_full_s3_iceberg(
    spark: Any,
    full_s3_data: Any,
    src_target_table_name: str,
    awsbucket: str,
    source: str,
) -> None:
    """Append full-load data into an Iceberg-backed bronze dataset.

    Args:
        spark: Active Spark session.
        full_s3_data: Dataframe to persist.
        src_target_table_name: Target dataset name.
        awsbucket: Bucket name used to derive the Iceberg database.
        source: Source system name used for table prefixing.
    """
    iceberg_database = awsbucket.replace("-", "_")
    if source.upper() == "CASSANDRA":
        prefix_src_target_table_name = "cs_" + src_target_table_name
    elif source.upper() == "POSTGRESQL":
        prefix_src_target_table_name = "pg_" + src_target_table_name
    elif source.upper() == "MYSQL":
        prefix_src_target_table_name = "ms_" + src_target_table_name
    elif source.upper() == "AZURESQL":
        prefix_src_target_table_name = "sqlsrvr_" + src_target_table_name
    else:
        prefix_src_target_table_name = src_target_table_name

    print(
        f'WRITING DATA IN ICEBERG DATABASE ""{iceberg_database}"" FOR TABLE ""{prefix_src_target_table_name}""'
    )

    # converting columns to lowercase
    for col_name in full_s3_data.columns:
        full_s3_data = full_s3_data.withColumnRenamed(col_name, col_name.lower())

    for col_dtypes in full_s3_data.dtypes:
        col_name = col_dtypes[0]
        if col_dtypes[1] == "timestamp":
            full_s3_data = full_s3_data.withColumn(
                col_name, lit(col(col_name).cast("timestamp"))
            )

    print("Schema of final data to be loaded in iceberg table")
    full_s3_data.printSchema()

    # full_s3_data.createOrReplaceGlobalTempView("iceberg_table_data")

    # Table creation
    # query_create_tab_raw = f"""
    #          CREATE TABLE IF NOT EXISTS my_icbg_catalog.{iceberg_database}.{prefix_src_target_table_name}
    #          USING iceberg
    #          TBLPROPERTIES ('write.distribution-mode'='hash','write.parquet.compression-codec'='snappy','format-version'='2', 'write.spark.accept-any-schema'='true')
    #         AS SELECT * FROM global_temp.iceberg_table_data
    #         limit 0
    #         """

    # spark.sql(query_create_tab_raw)
    # print("Table Creation Successful")

    # spark.sql(f""" show create table my_icbg_catalog.{iceberg_database}.{prefix_src_target_table_name}""").show(truncate=False)
    # quit()
    # exit()

    # insert query
    '''query_insert_data = f"""
            INSERT INTO my_icbg_catalog.{iceberg_database}.{prefix_src_target_table_name}
            SELECT * FROM global_temp.iceberg_table_data
            """

    spark.sql(query_insert_data)'''

    full_s3_data.writeTo(
        f"my_icbg_catalog.{iceberg_database}.{prefix_src_target_table_name}"
    ).option("mergeSchema", "true").append()

    print(f"Iceberg Table Data updated for: {prefix_src_target_table_name}")


def write_full_iceberg_table(
    spark: Any, full_s3_iceberg_data: Any, src_target_table_name: str, awsbucket: str
) -> None:
    """Replace an Iceberg table with a full-load dataframe snapshot.

    Args:
        spark: Active Spark session.
        full_s3_iceberg_data: Dataframe to persist.
        src_target_table_name: Target Iceberg table name.
        awsbucket: Bucket name used to derive the Iceberg database.
    """
    iceberg_database = awsbucket.replace("-", "_")

    # converting columns to lowercase
    for col_name in full_s3_iceberg_data.columns:
        full_s3_iceberg_data = full_s3_iceberg_data.withColumnRenamed(
            col_name, col_name.lower()
        )

    print("Schema of final data to be loaded in iceberg table")
    full_s3_iceberg_data.printSchema()

    try:
        delete_full_table_data_query = f"""
        delete from my_icbg_catalog.{iceberg_database}.{src_target_table_name}
        """
        spark.sql(delete_full_table_data_query)

        print(
            f"Deletion of all records for table {iceberg_database}.{src_target_table_name} is complete"
        )
        print("Proceeding for full data load.....")

    except Exception as e:
        logging.exception(
            f"exception while deleting all table records for {iceberg_database}.{src_target_table_name}:"
            + str(e)
        )
        raise e

    try:

        full_s3_iceberg_data.createOrReplaceGlobalTempView("iceberg_full_load_data")

        full_load_query = f"""
            INSERT INTO my_icbg_catalog.{iceberg_database}.{src_target_table_name}
            SELECT * FROM global_temp.iceberg_full_load_data
        """

        spark.sql(full_load_query)

    except Exception as e:
        print(
            "************FULL LOAD FAILED HENCE REVERTING TO PREVIOUS SNAPSHOT(JUST BEFORE DELETE) for below table:"
        )
        print(f"************{iceberg_database}.{src_target_table_name}")
        logging.exception(
            f"exception while doing full load in table: {iceberg_database}.{src_target_table_name}:"
            + str(e)
        )
        icbg_revert_to_previous_snapshot(spark, iceberg_database, src_target_table_name)
        raise e

    print(f"Iceberg Table Data updated for: {iceberg_database}.{src_target_table_name}")
    spark.catalog.dropGlobalTempView("iceberg_full_load_data")


def read_sf_meta_table(
    spark, SNOWFLAKE_SOURCE_NAME, keyspace, sfOptionsRaw, processing_grp, awsbucket
):
    """Read active bronze object metadata from Snowflake.

    Args:
        spark: Active Spark session.
        SNOWFLAKE_SOURCE_NAME: Spark Snowflake connector format name.
        keyspace: Source namespace or keyspace.
        sfOptionsRaw: Snowflake connection options.
        processing_grp: Processing group to filter by.
        awsbucket: S3 bucket name carried through the workflow.

    Returns:
        A filtered metadata dataframe for the current run.
    """
    try:

        df_meta_map_src_tgt_obj = (
            spark.read.format(SNOWFLAKE_SOURCE_NAME)
            .options(**sfOptionsRaw)
            .option("dbtable", "PIPELINE_CONFIG")
            .load()
        )

        temp_df = (
            df_meta_map_src_tgt_obj.where(
                (df_meta_map_src_tgt_obj["SOURCE_NAMESPACE"] == keyspace)
                & (df_meta_map_src_tgt_obj["LAYER_TYPE"] == "BRONZE")
            )
            .where(df_meta_map_src_tgt_obj["IS_ACTIVE"] == "TRUE")
            .where(df_meta_map_src_tgt_obj["PROC_GROUP"] == processing_grp)
        )

        return temp_df

    except Exception as e:
        logging.exception(
            "exception when reading PIPELINE_CONFIG Table :" + str(e)
        )
        raise e


def read_iceberg_meta_obj_table(
    spark: Any, iceberg_db: Optional[str], keyspace: str, process_grp: str
) -> Any:
    """Read active bronze object metadata from an Iceberg configuration table.

    Args:
        spark: Active Spark session.
        iceberg_db: Iceberg database that stores the config table.
        keyspace: Source namespace or database name.
        process_grp: Processing group to filter by.

    Returns:
        A filtered metadata dataframe for the current run.
    """

    try:
        if iceberg_db is None:
            raise Exception(
                '\nPlease provide iceberg config table database eg: "config_table_db":"<database_name>"\n'
            )

        print(
            f"iceberg table name is: my_icbg_catalog.{iceberg_db}.pipeline_config"
        )
        df_meta_map_src_tgt_obj_icbg = spark.read.format("iceberg").load(
            f"my_icbg_catalog.{iceberg_db}.pipeline_config"
        )

        temp_df = (
            df_meta_map_src_tgt_obj_icbg.where(
                (df_meta_map_src_tgt_obj_icbg["SOURCE_NAMESPACE"] == keyspace)
                & (df_meta_map_src_tgt_obj_icbg["LAYER_TYPE"] == "BRONZE")
            )
            .where(df_meta_map_src_tgt_obj_icbg["IS_ACTIVE"] == "TRUE")
            .where(df_meta_map_src_tgt_obj_icbg["PROC_GROUP"] == process_grp)
        )

        return temp_df

    except Exception as e:
        logging.exception(
            "exception when reading 'ICEBERG' PIPELINE_CONFIG Table :" + str(e)
        )
        raise e


def get_list_table_meta_obj_sf(sf_meta_map_src_obj: Any) -> List[str]:
    """Collect distinct source record names from Snowflake metadata.

    Args:
        sf_meta_map_src_obj: Metadata dataframe.

    Returns:
        A list of source table names.
    """
    sf_meta_map_src_obj = sf_meta_map_src_obj.where(
        (sf_meta_map_src_obj["LAYER_TYPE"] == "BRONZE")
    )
    collected_table = (
        sf_meta_map_src_obj.select("SOURCE_TABLE_NAME").distinct().toPandas()
    )
    table_names = list(collected_table["SOURCE_TABLE_NAME"])
    return table_names


def get_list_table_meta_obj_iceberg(icbg_meta_map_src_obj: Any) -> List[str]:
    """Collect distinct source record names from Iceberg metadata.

    Args:
        icbg_meta_map_src_obj: Metadata dataframe.

    Returns:
        A list of source table names.
    """
    icbg_meta_map_src_obj = icbg_meta_map_src_obj.where(
        (icbg_meta_map_src_obj["LAYER_TYPE"] == "BRONZE")
    )
    collected_table = (
        icbg_meta_map_src_obj.select("SOURCE_TABLE_NAME").distinct().toPandas()
    )
    table_names = list(collected_table["SOURCE_TABLE_NAME"])
    return table_names


def load_meta_obj_table(
    spark: Any, SNOWFLAKE_SOURCE_NAME: str, sfOptionsRaw: Any, table_name: str
) -> Any:
    """Load the full Snowflake metadata table used by bronze ingestion.

    Args:
        spark: Active Spark session.
        SNOWFLAKE_SOURCE_NAME: Spark Snowflake connector format name.
        sfOptionsRaw: Snowflake connection options.
        table_name: Source table name carried by callers.

    Returns:
        The full metadata dataframe.
    """
    try:
        df_meta_map_src_tgt_obj = (
            spark.read.format(SNOWFLAKE_SOURCE_NAME)
            .options(**sfOptionsRaw)
            .option("dbtable", "PIPELINE_CONFIG")
            .load()
        )
        return df_meta_map_src_tgt_obj

    except Exception as e:
        logging.exception(
            "exception when reading snowflake PIPELINE_CONFIG Table :" + str(e)
        )
        raise e


def get_primary_key_sf_meta_table(
    spark: Any, SNOWFLAKE_SOURCE_NAME: str, sfOptionsRaw: Any, table_name: str
) -> Tuple[List[str], Any]:
    """Extract partition and clustering keys for a source object from metadata.

    Args:
        spark: Active Spark session.
        SNOWFLAKE_SOURCE_NAME: Spark Snowflake connector format name.
        sfOptionsRaw: Snowflake connection options.
        table_name: Source table name.

    Returns:
        A tuple of primary-key column names and the metadata dataframe.
    """
    try:
        df_meta_map_src_tgt_obj = (
            spark.read.format(SNOWFLAKE_SOURCE_NAME)
            .options(**sfOptionsRaw)
            .option("dbtable", "PIPELINE_CONFIG")
            .load()
        )

        temp_df = df_meta_map_src_tgt_obj.where(
            df_meta_map_src_tgt_obj["SOURCE_TABLE_NAME"] == table_name
        )
        resource_key = temp_df.select("SOURCE_KEY_FIELDS").toPandas()
        pk_name_list = list(resource_key["SOURCE_KEY_FIELDS"])
        for pk in pk_name_list:
            res = json.loads(pk)
            ck_value = res.get("clustering_key")
            pk_value = res.get("partition_key")
            primary_key = pk_value + "," + ck_value
            pk_key_list = primary_key.split(",")
            return pk_key_list, df_meta_map_src_tgt_obj

    except Exception as e:
        logging.exception(
            "exception when reading PIPELINE_CONFIG Table for PK:" + str(e)
        )
        raise e


def get_target_table_name(
    df_meta_map_src_tgt_obj: Any, table_name: str
) -> Optional[str]:
    """Resolve the target object name for a source record type.

    Args:
        df_meta_map_src_tgt_obj: Metadata dataframe.
        table_name: Source table name.

    Returns:
        The mapped target object name when present.
    """
    temp_df = df_meta_map_src_tgt_obj.where(
        df_meta_map_src_tgt_obj["SOURCE_TABLE_NAME"] == table_name
    )
    collected_target = temp_df.select("TARGET_TABLE_NAME").toPandas()
    target_table = list(collected_target["TARGET_TABLE_NAME"])
    for tg_table in target_table:
        return tg_table


def get_meta_obj_field(
    df_meta_map_src_tgt_obj: Any, table_name: str, processinggrp: str
) -> Tuple[str, str, str, str, str, str, str]:
    """Collect the bronze and lander control fields for a source object.

    Args:
        df_meta_map_src_tgt_obj: Metadata dataframe.
        table_name: Source table name.
        processinggrp: Processing group to filter by.

    Returns:
        A tuple of pull method, flattening type, source processing value,
        lander target name, bronze source identifier, bronze target name,
        and target object type.
    """
    temp_df = df_meta_map_src_tgt_obj.where(
        df_meta_map_src_tgt_obj["SOURCE_TABLE_NAME"] == table_name
    )
    temp_df = temp_df.where(
        df_meta_map_src_tgt_obj["PROC_GROUP"] == processinggrp
    )

    print("df_meta_map_src_tgt_obj")
    temp_df.show(n=30, truncate=False)
    bronze_df = temp_df.where(f.col("LAYER_TYPE") == "BRONZE")

    print("BRONZE ROW")
    bronze_df.show()

    df_ingestion_pull = bronze_df.select("EXTRACT_METHOD").toPandas()

    pull_ingestion = list(df_ingestion_pull["EXTRACT_METHOD"])

    pull_ingestion_method = pull_ingestion[0]

    print("pull_ingestion_method", pull_ingestion_method)

    df_trgt_object_type = bronze_df.select("TARGET_STORE_TYPE").toPandas()

    target_object_type = list(df_trgt_object_type["TARGET_STORE_TYPE"])

    target_object = target_object_type[0]

    print("target_object", target_object)

    df_bronze_source = bronze_df.select("SOURCE_SYSTEM").toPandas()
    bronze_source = list(df_bronze_source["SOURCE_SYSTEM"])
    bronze_source = bronze_source[0]

    print("bronze_source", bronze_source)

    target_table_name_s3 = bronze_df.select("TARGET_TABLE_NAME").toPandas()
    df_target_name_s3 = list(target_table_name_s3["TARGET_TABLE_NAME"])
    src_target_table_name = df_target_name_s3[0]

    print("src_target_table_name", src_target_table_name)

    lander_df = temp_df.where(f.col("LAYER_TYPE") == "LANDER")

    if len(lander_df.head(1)) == 0:
        print("empty ")
        flattening_type = "NA"
        src_processing_value = "NA"
        df_target_name_sf = "NA"
    else:

        flattening_type_df = lander_df.select("FLATTEN_MODE").toPandas()

        flattening_type = list(flattening_type_df["FLATTEN_MODE"])

        flattening_type = flattening_type[0]

        print(flattening_type)

        df_src_processing_value = lander_df.select(
            "CHECKPOINT_FROM_VALUE"
        ).toPandas()
        src_processing_value = list(
            df_src_processing_value["CHECKPOINT_FROM_VALUE"]
        )
        src_processing_value = src_processing_value[0]

        df_TARGET_TABLE_NAME = lander_df.select("TARGET_TABLE_NAME").toPandas()
        df_target_name_sf = list(df_TARGET_TABLE_NAME["TARGET_TABLE_NAME"])
        df_target_name_sf = df_target_name_sf[0]

        print("flattening_type>>", flattening_type)

    return (
        pull_ingestion_method,
        flattening_type,
        src_processing_value,
        df_target_name_sf,
        bronze_source,
        src_target_table_name,
        target_object,
    )


def load_meta_log_processing(
    spark: Any, SNOWFLAKE_SOURCE_NAME: str, sfOptionsRaw: Any
) -> Any:
    """Load the Snowflake meta-log processing table.

    Args:
        spark: Active Spark session.
        SNOWFLAKE_SOURCE_NAME: Spark Snowflake connector format name.
        sfOptionsRaw: Snowflake connection options.

    Returns:
        A Spark dataframe of meta-log records.
    """
    try:
        df_meta_log_f_processing = (
            spark.read.format(SNOWFLAKE_SOURCE_NAME)
            .options(**sfOptionsRaw)
            .option("dbtable", "file_ingest_log")
            .load()
        )
        return df_meta_log_f_processing

    except Exception as e:
        logging.exception(
            "exception when reading PIPELINE_CONFIG Table for PK:" + str(e)
        )
        raise e


def prefix_for_bronze(awsbucket: str, inc_path: str) -> str:
    """Strip the bucket prefix from a fully qualified S3 path.

    Args:
        awsbucket: S3 bucket name.
        inc_path: Fully qualified S3 path.

    Returns:
        The key prefix relative to the bucket.
    """
    sub_str = awsbucket + "/"
    prefix = inc_path.split(sub_str, 1)[1]
    return prefix


def meta_log_processing_df(
    spark: Any, prefix: str, awsbucket: str, table_name: str
) -> Any:
    """Build a dataframe of S3 objects suitable for meta-log ingestion.

    Args:
        spark: Active Spark session.
        prefix: S3 key prefix to scan.
        awsbucket: Bucket name.
        table_name: Source record type name.

    Returns:
        A Spark dataframe shaped like the Snowflake meta-log table.
    """
    try:
        s3_client = boto3.client("s3")
    except Exception as e:
        logging.exception("Exception raised by boto3 connecting to s3:" + str(e))
        raise e

    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=awsbucket, Prefix=prefix)
    bucket_list = []

    for page in page_iterator:

        for item in page["Contents"]:
            listConcat = item["Key"] + "?" + str(item["LastModified"])
            bucket_list.append(listConcat)

    pandas_df = pd.DataFrame(bucket_list, columns=["RawColumn"])
    df_to_spark_compatible = spark.createDataFrame(pandas_df)
    df_bucket_path_split_to_col = (
        df_to_spark_compatible.withColumn(
            "BucketPath", f.split(df_to_spark_compatible["RawColumn"], r"\\?")[0]
        )
        .withColumn(
            "LastModified", f.split(df_to_spark_compatible["RawColumn"], r"\\?")[1]
        )
        .drop("RawColumn")
    )

    df_lastmod_cast_ts = df_bucket_path_split_to_col.withColumn(
        "LastModified", df_bucket_path_split_to_col["LastModified"].cast("timestamp")
    )

    # remove Unwanted column from s3 function
    df_s3_path_col_renaming_process_flag = (
        df_lastmod_cast_ts.withColumnRenamed("BucketPath", "FILE_WITH_PATH")
        .withColumnRenamed("LastModified", "FILE_MODIFIED_DTIMEUTC")
        .withColumn("PROCESSED_FLAG", lit("False"))
    )

    df_valid_path_file_uid = df_s3_path_col_renaming_process_flag.withColumn(
        "File_UID", f.md5(df_s3_path_col_renaming_process_flag["FILE_WITH_PATH"])
    )

    df_sf_meta_log_wrt = df_valid_path_file_uid.filter(
        ~df_valid_path_file_uid["FILE_WITH_PATH"].rlike("_SUCCESS")
    )

    from datetime import datetime

    dt = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    df = (
        df_sf_meta_log_wrt.withColumn("PROCESSED_FLAG", lit("False"))
        .withColumn("BUCKET_NAME", lit(awsbucket))
        .withColumn("FILE_PROCESSED_DTIMEUTC", lit(dt).cast("timestamp"))
        .withColumn("TABLE_NAME", lit(table_name))
        .withColumn("FILE_ERROR_MSG", lit("NA"))
        .withColumn("MAP_SRC_TRGT_OBJECT_UID", lit("NA"))
        .withColumn("CREATED_AT_DTIMEUTC", lit(dt).cast("timestamp"))
        .withColumn("REPROCESS_FLAG", lit("False"))
    )

    return df


def map_df_sf_col(df: Any) -> str:
    """Generate the Snowflake `columnmap` string for a dataframe.

    Args:
        df: Dataframe whose columns should be mapped one-to-one.

    Returns:
        A Snowflake connector column mapping string.
    """
    dflist = df.columns
    dflistlen = len(dflist)
    count = 0
    list2 = []
    c1 = 0
    while count < dflistlen:
        list2.append(dflist[c1] + " -> " + dflist[c1])
        count = count + 1
        c1 = c1 + 1

    liststr = str(list2)
    listReplace = (
        liststr.replace("[", "(").replace("]", ")").replace('"', "").replace("\"'", "")
    )
    listSub = re.sub("'", "", listReplace)
    mapConcat = "Map"
    listStrConcat = mapConcat + listSub

    return listStrConcat


def write_df_sf_meta_log_process_false(
    df: Any, SNOWFLAKE_SOURCE_NAME: str, sfOptionsRaw: Any
) -> None:
    """Append meta-log rows to Snowflake with `PROCESSED_FLAG` set to false.

    Args:
        df: Meta-log dataframe to persist.
        SNOWFLAKE_SOURCE_NAME: Spark Snowflake connector format name.
        sfOptionsRaw: Snowflake connection options.
    """
    try:
        mapStrinfForMetaLogException = map_df_sf_col(df)
        df.write.format(SNOWFLAKE_SOURCE_NAME).option(
            "dbtable", "FILE_INGEST_LOG"
        ).option("columnmap", mapStrinfForMetaLogException).options(
            **sfOptionsRaw
        ).mode(
            "append"
        ).save()

    except Exception as e:
        logging.exception(
            "Exception: writing into meta log , setting processing flag false :"
            + str(e)
        )
        raise e


def full_load_data_backup(aws_bucket: str, src_table: str) -> None:
    """Copy the current full-load dataset into the rolling S3 backup area.

    Args:
        aws_bucket: S3 bucket name.
        src_table: Source table name.
    """

    old_bucket_name = aws_bucket
    old_prefix = src_table + "/" + "full_load/"
    new_bucket_name = aws_bucket

    d = datetime.datetime.now()
    year = f"{d.year}"
    month = f"{d.month:02d}"
    day = f"{d.day:02d}"
    hour = f"{d.hour:02d}"

    new_prefix = (
        src_table
        + "/"
        + "tmp"
        + "/"
        + str(year)
        + "/"
        + str(month)
        + "/"
        + str(day)
        + "/"
        + str(hour)
        + "/"
    )

    s3 = boto3.resource("s3")
    old_bucket = s3.Bucket(old_bucket_name)
    new_bucket = s3.Bucket(new_bucket_name)

    for obj in old_bucket.objects.filter(Prefix=old_prefix):
        old_source = {"Bucket": old_bucket_name, "Key": obj.key}
        # replace the prefix
        new_key = obj.key.replace(old_prefix, new_prefix, 1)
        new_obj = new_bucket.Object(new_key)
        new_obj.copy(old_source)

    tmp_prefix = src_table + "/" + "tmp/"

    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=aws_bucket, Prefix=tmp_prefix)
    last_modfied_time = []

    for page in page_iterator:

        for item in page["Contents"]:
            dt_tz = item["LastModified"]
            dt = dt_tz.replace(tzinfo=None).replace(minute=0, second=0)
            last_modfied_time.append(dt)

    last_modfied_time = list(set(last_modfied_time))
    last_modfied_time.sort()

    if len(last_modfied_time) > 3:
        third_recent_run_date = last_modfied_time[-3]

        for page in page_iterator:

            for item in page["Contents"]:
                key = item["Key"]
                last_mod = item["LastModified"]
                del_dt = last_mod.replace(tzinfo=None)

                flag = del_dt < third_recent_run_date

                if flag:
                    s3_client.delete_object(Bucket=aws_bucket, Key=key)
                    print("delete done for key ", key)
    else:
        print(
            "No S3 tmp folder files deleted in order to keep data from last three runs"
        )


def hash_rename(df: Any) -> Any:
    """Avoid collisions with the framework `hash` column by renaming source columns.

    Args:
        df: Source dataframe.

    Returns:
        The dataframe with any case-insensitive `hash` column renamed to `src_hash`.
    """
    for col_name in list(df.columns):
        print("hash column checking >>>>>>>>>", col_name)
        if "hash" == col_name.casefold():
            df = df.withColumnRenamed(col_name, "src_hash")
            break
    return df


def ingest_dt_in_df(src_df: Any, dt: Any) -> Any:
    """Add the ingest effective timestamp to a dataframe.

    Args:
        src_df: Source dataframe.
        dt: Timestamp value to stamp into the dataframe.

    Returns:
        The dataframe with `INGEST_EFF_DTIMEUTC` added.
    """
    src_dt_ingest_dt = src_df.withColumn("INGEST_EFF_DTIMEUTC", lit(dt))
    return src_dt_ingest_dt


def s3_path_for_key_based(bucket_name: str, table_name: str) -> str:
    """Build the S3 output path for key-based incremental extracts.

    Args:
        bucket_name: Target S3 bucket name.
        table_name: Source table name.

    Returns:
        The fully qualified S3 path for the current hour partition.
    """
    d = datetime.datetime.now()
    year = f"{d.year}"
    month = f"{d.month:02d}"
    day = f"{d.day:02d}"
    hour = f"{d.hour:02d}"

    key_data_path = "s3a://" + bucket_name + "/" + table_name + "/" + "key_incr_load/"

    key_based_s3_path = (
        key_data_path
        + str(year)
        + "/"
        + str(month)
        + "/"
        + str(day)
        + "/"
        + str(hour)
        + "/"
    )

    return key_based_s3_path


def key_inc_s3_write(key_inc_data: Any, key_s3_path: str, no_of_out_files: int) -> None:
    """Write key-based incremental output to S3.

    Args:
        key_inc_data: Incremental dataframe to persist.
        key_s3_path: Target S3 parquet path.
        no_of_out_files: Number of output files to emit.
    """
    try:

        inc_data_s3 = key_inc_data.coalesce(no_of_out_files)
        print("inc_path", key_s3_path)
        inc_data_s3.write.parquet(key_s3_path)

    except Exception as e:
        logging.exception(
            "exception during writing key based incremental data into s3 " + str(e)
        )
        raise e


def key_inc_iceberg_write(
    spark: Any, key_incr_icbg_df: Any, trgt_table_name: str, awsbucket: str
) -> None:
    """Append key-based incremental records into an Iceberg dataset.

    Args:
        spark: Active Spark session.
        key_incr_icbg_df: Dataframe to persist.
        trgt_table_name: Target Iceberg table name.
        awsbucket: Bucket name used to derive the Iceberg database.
    """
    iceberg_database = awsbucket.replace("-", "_")

    key_incr_icbg_df = key_incr_icbg_df.drop(f.col("hash"))

    print(
        f'WRITING KEY BASED DATA IN ICEBERG DATABASE ""{iceberg_database}"" FOR TABLE ""{trgt_table_name}""'
    )

    # converting columns to lowercase
    for col_name in key_incr_icbg_df.columns:
        key_incr_icbg_df = key_incr_icbg_df.withColumnRenamed(
            col_name, col_name.lower()
        )

    for col_dtypes in key_incr_icbg_df.dtypes:
        col_name = col_dtypes[0]
        if col_dtypes[1] == "timestamp":
            key_incr_icbg_df = key_incr_icbg_df.withColumn(
                col_name, lit(col(col_name).cast("timestamp"))
            )

    print("Schema of final data to be loaded in iceberg table")
    key_incr_icbg_df.printSchema()

    try:
        key_incr_icbg_df.writeTo(
            f"my_icbg_catalog.{iceberg_database}.{trgt_table_name}"
        ).option("mergeSchema", "true").append()

    except Exception as e:
        logging.exception(
            f"exception in key_inc_iceberg_write while doing key increment load in iceberg table: \
                            {iceberg_database}.{trgt_table_name}:"
            + str(e)
        )
        raise e

    print(
        f"Iceberg Table Key Increment Data updated for: {iceberg_database}.{trgt_table_name}"
    )
    spark.catalog.dropGlobalTempView("iceberg_key_increment_data")


def get_incrmt_fld(pipeline_cfg_df: Any, table_name: str) -> List[str]:
    """Extract checkpoint field names for a key-based incremental dataset.

    Args:
        pipeline_cfg_df: Metadata dataframe.
        table_name: Target table name.

    Returns:
        A list of source field names used as incremental checkpoints.
    """
    temp_df = pipeline_cfg_df.where(
        pipeline_cfg_df["TARGET_TABLE_NAME"] == table_name
    )

    bronze_df = temp_df.where(f.col("LAYER_TYPE") == "BRONZE")

    df_prc_fld_name = bronze_df.select("CHECKPOINT_FIELD_DEF").toPandas()
    processing_fld_value = list(df_prc_fld_name["CHECKPOINT_FIELD_DEF"])
    src_obj_processing_field_value = processing_fld_value[0]

    src_obj_processing_field_value_lt = src_obj_processing_field_value.split("|")

    src_obj_fld_name = []
    for el in src_obj_processing_field_value_lt:
        src_obj_fld_name.append(el.split("::")[0])

    return src_obj_fld_name


def last_str_trim(string: str) -> str:
    """Trim the trailing pipe delimiter from a serialized checkpoint value.

    Args:
        string: Serialized checkpoint string.

    Returns:
        The string with a trailing `|` removed when present.
    """
    ending = "|"
    if string.endswith(ending):
        return string[: -len(ending)]
    return string


def src_obj_to_fld_update_val_icbg(
    key_based_df: Any, fld_name: Sequence[str], source: str
) -> str:
    """Serialize the max checkpoint value(s) for Iceberg config updates.

    Args:
        key_based_df: Incremental dataframe.
        fld_name: Source checkpoint field names.
        source: Source system name.

    Returns:
        A quoted checkpoint string formatted for SQL updates.
    """
    max_val = []
    for cl_nam in fld_name:
        max_val.append(key_based_df.agg({cl_nam: "max"}).collect()[0][0])

    sac_to_fad_update = ""

    max_val_icbg = []
    for i in max_val:
        j = str(i)
        max_val_icbg.append(j)

    for vl in max_val_icbg:
        if len(max_val_icbg) == 1:
            sac_to_fad_update = str(vl)
        else:
            sac_to_fad_update = sac_to_fad_update + str(vl) + "|"
            sac_to_fad_update = last_str_trim(sac_to_fad_update)

    sac_to_fad_update = "'" + sac_to_fad_update + "'"

    return sac_to_fad_update


def src_obj_to_fld_update_val(
    key_based_df: Any, fld_name: Sequence[str], source: str
) -> str:
    """Serialize the max checkpoint value(s) for Snowflake config updates.

    Args:
        key_based_df: Incremental dataframe.
        fld_name: Source checkpoint field names.
        source: Source system name.

    Returns:
        A quoted checkpoint string formatted for SQL updates.
    """
    max_val = []
    for cl_nam in fld_name:
        max_val.append(key_based_df.agg({cl_nam: "max"}).collect()[0][0])

    sac_to_fad_update = ""

    max_val_sf = []
    for i in max_val:
        j = str(i)
        if source == "CASSANDRA":
            max_val_sf.append(j)
        else:
            max_val_sf.append(j.split(".")[0])

    for vl in max_val_sf:
        if len(max_val_sf) == 1:
            sac_to_fad_update = str(vl)
        else:
            sac_to_fad_update = sac_to_fad_update + str(vl) + "|"
            sac_to_fad_update = last_str_trim(sac_to_fad_update)

    sac_to_fad_update = "'" + sac_to_fad_update + "'"

    return sac_to_fad_update


def src_to_fld_update_sf(
    spark,
    src_to_fld,
    sf_db_raw,
    sf_connection_raw,
    product_app,
    record_name,
    namespace,
    is_empty_flag,
):
    """Update Snowflake metadata with the latest key-based checkpoint value.

    Args:
        spark: Active Spark session.
        src_to_fld: Serialized checkpoint value.
        sf_db_raw: Snowflake raw database name.
        sf_connection_raw: Snowflake connection options.
        product_app: Snowflake schema name.
        record_name: Source table name.
        namespace: Source namespace or database.
        is_empty_flag: Indicates whether the incremental dataframe is empty.
    """

    if is_empty_flag != 0:

        update_query_to_value = (
            f"update {sf_db_raw}." + product_app + ".PIPELINE_CONFIG set "
            "CHECKPOINT_TO_VALUE = "
            ""
            + src_to_fld
            + " where LAYER_TYPE = 'BRONZE' and  SOURCE_NAMESPACE = "
            + "'"
            + namespace
            + "'"
            + " and SOURCE_TABLE_NAME = '"
            + record_name
            + "' "
        )

        print("update_query_to_value", update_query_to_value)
        spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            sf_connection_raw, update_query_to_value
        )
    else:
        print(
            "CHECKPOINT_TO_VALUE column not updated in table PIPELINE_CONFIG\
         since there are no new records"
        )


def src_to_fld_update_icbg(
    spark,
    src_to_fld_update,
    is_empty_flag,
    config_table_database,
    namespace,
    record_name,
    target_dataset_table,
    awsbucket,
):
    """Update Iceberg metadata with the latest key-based checkpoint value.

    Args:
        spark: Active Spark session.
        src_to_fld_update: Serialized checkpoint value.
        is_empty_flag: Indicates whether the incremental dataframe is empty.
        config_table_database: Iceberg database containing config tables.
        namespace: Dataset source namespace or database.
        record_name: Dataset source table name.
        target_dataset_table: Target Iceberg table receiving the incremental load.
        awsbucket: Bucket name used to derive the Iceberg database.
    """

    if is_empty_flag != 0:

        try:

            update_query_to_value = f"""
                update my_icbg_catalog.{config_table_database}.pipeline_config
                set CHECKPOINT_TO_VALUE = {src_to_fld_update}
                where LAYER_TYPE = 'BRONZE'
                and  SOURCE_NAMESPACE = '{namespace}'
                and SOURCE_TABLE_NAME = '{record_name}'
                """

            print("update_query_to_value", update_query_to_value)
            spark.sql(update_query_to_value)

        except Exception as e:
            iceberg_database_trgt_dataset = awsbucket.replace("-", "_")
            print(
                "****FAILURE IN UPDATING pipeline_config FIELD 'CHECKPOINT_TO_VALUE' for below table:"
            )
            print(f"{iceberg_database_trgt_dataset}.{target_dataset_table}")
            print(
                f"HENCE REVERTING TABLE TO PREVIOUS SNAPSHOT(JUST BEFORE 'KEY_INCR_EXTRACT' DATA LOAD)"
            )

            icbg_revert_to_previous_snapshot(
                spark, iceberg_database_trgt_dataset, target_dataset_table
            )

            logging.exception(
                f"Exception in src_to_fld_update_icbg while updating CHECKPOINT_TO_VALUE"
                + str(e)
            )
            raise e

    else:
        print(
            "CHECKPOINT_TO_VALUE column not updated in ICEBERG table PIPELINE_CONFIG\
         since there are no new records"
        )


def get_secret(secretname: str, region_name: str) -> Tuple[str, str]:
    """Read Snowflake credentials from AWS Secrets Manager.

    Args:
        secretname: Name of the AWS secret.
        region_name: AWS region that holds the secret.

    Returns:
        A tuple of Snowflake username and password.
    """
    secret_name = secretname
    # region_name = "us-east-1"
    print("AWS Region name is: ", region_name)

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    except ClientError as e:
        if e.response["Error"]["Code"] == "DecryptionFailureException":
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceErrorException":
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = json.loads(get_secret_value_response["SecretString"])
            sfUser = secret["sfUser"]
            sfPassword = secret["sfPassword"]

            return sfUser, sfPassword
        else:
            decoded_binary_secret = base64.b64decode(
                get_secret_value_response["SecretBinary"]
            )
