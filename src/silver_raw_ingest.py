import logging
import pyspark.sql.functions as f
from bronze_lib import meta_log_processing_df
from pyspark.sql.functions import split
from typing import Any, List, Sequence, Tuple


def prepare_data(data: Any) -> Any:
    """Serialize a dataframe row into the raw `SRC` JSON payload format.

    Args:
        data: Source dataframe to convert.

    Returns:
        A dataframe containing only the serialized `SRC` column.
    """
    return data.withColumn(
        "SRC", f.to_json(f.struct([data[x] for x in data.columns]))
    ).select("SRC")


def write_into_sf(
    spark: Any,
    sf_data: Any,
    table_name: str,
    SNOWFLAKE_SOURCE_NAME: str,
    sfoptionsraw: Any,
) -> None:
    """Replace the target Snowflake table contents with the provided dataframe.

    Args:
        spark: Active Spark session.
        sf_data: Dataframe to persist to Snowflake.
        table_name: Target Snowflake table name.
        SNOWFLAKE_SOURCE_NAME: Spark Snowflake connector format name.
        sfoptionsraw: Snowflake connection options.
    """
    try:
        truncate_tab_query = "truncate " + table_name
        spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            sfoptionsraw, truncate_tab_query
        )

        sf_data.write.format(SNOWFLAKE_SOURCE_NAME).option(
            "dbtable", table_name
        ).options(**sfoptionsraw).mode("append").save()
    except Exception as e:
        print("in exception")
        logging.exception("exception during writing into sf table >>>>>:" + str(e))
        raise e


def read_meta_log_processing(
    spark: Any, SNOWFLAKE_SOURCE_NAME: str, table_name: str, sfoptionsraw: Any
) -> List[str]:
    """Collect unprocessed lander file paths for a single record type.

    Args:
        spark: Active Spark session.
        SNOWFLAKE_SOURCE_NAME: Spark Snowflake connector format name.
        table_name: Record type to filter in the meta log table.
        sfoptionsraw: Snowflake connection options.

    Returns:
        A list of S3 file paths pending lander processing.
    """
    try:
        df_meta_log_processing = (
            spark.read.format(SNOWFLAKE_SOURCE_NAME)
            .options(**sfoptionsraw)
            .option("dbtable", "FILE_INGEST_LOG")
            .load()
        )
        temp_df = df_meta_log_processing.where(
            df_meta_log_processing["TABLE_NAME"] == table_name
        ).where(df_meta_log_processing["PROCESSED_FLAG"] == "FALSE")

        df = temp_df.withColumn(
            "s3_file_path",
            f.concat(
                f.lit("s3a://"),
                f.col("BUCKET_NAME"),
                f.lit("/"),
                f.col("FILE_WITH_PATH"),
            ),
        )

        collected_files = df.select("s3_file_path").toPandas()
        file_path_list = list(collected_files["s3_file_path"])

        return file_path_list

    except Exception as e:
        print("in exception")
        logging.exception("exception during meta log processing >>>>>:" + str(e))
        raise e


def meta_log_flag_update(
    spark: Any,
    sfDatabaseRaw: str,
    sfSchema: str,
    sfoptionsraw: Any,
    SNOWFLAKE_SOURCE_NAME: str,
    list_of_fileid: Sequence[str],
) -> None:
    """Mark processed file identifiers as complete in Snowflake metadata.

    Args:
        spark: Active Spark session.
        sfDatabaseRaw: Snowflake raw database name.
        sfSchema: Snowflake schema containing the meta log table.
        sfoptionsraw: Snowflake connection options.
        SNOWFLAKE_SOURCE_NAME: Spark Snowflake connector format name.
        list_of_fileid: File identifiers to mark as processed.
    """
    sr = ""
    for s in list_of_fileid:
        sr = sr + "'" + s + "'" + ","

    result = sr.rstrip(",")
    result1 = result.rstrip("'")
    final_uids = result1.lstrip("'")

    flag = "True"
    update_meta_log = (
        f"update  {sfDatabaseRaw}.{sfSchema}.FILE_INGEST_LOG  set PROCESSED_FLAG = "
        + flag
        + " where FILE_UID "
        f"in ('" + final_uids + "') "
    )
    spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
        sfoptionsraw, update_meta_log
    )


def get_list_parquet_files(
    spark: Any,
    src_date_field: Any,
    awsbucket: str,
    table: str,
    pull_ingest_method: str,
) -> Tuple[List[str], List[str]]:
    """Resolve parquet file ids and S3 paths for the current lander load.

    Args:
        spark: Active Spark session.
        src_date_field: Source checkpoint value used for incremental filtering.
        awsbucket: S3 bucket that stores bronze output files.
        table: Source table name.
        pull_ingest_method: Extraction mode that controls file selection.

    Returns:
        A tuple of file identifiers and fully qualified S3 parquet paths.
    """
    lst_files_df = meta_log_processing_df(spark, table, awsbucket, table)
    lst_files_df = lst_files_df.withColumn(
        "table_name", split(lst_files_df["FILE_WITH_PATH"], "/")[0]
    )
    lst_files_df = lst_files_df.where(lst_files_df["table_name"] == table).drop(
        "table_name"
    )

    if pull_ingest_method == "FULL_EXTRACT_INCR":
        df_files = lst_files_df.withColumn(
            "filter_files", split(f.col("FILE_WITH_PATH"), "/").getItem(1)
        )
        df_final = df_files.filter(f.col("filter_files") != "full_load")
        df_after_src_date = df_final.where(
            lst_files_df["FILE_MODIFIED_DTIMEUTC"] > src_date_field
        )
    else:

        df_files_date = (
            lst_files_df.withColumn(
                "year", split(f.col("FILE_WITH_PATH"), "/").getItem(2)
            )
            .withColumn("month", split(f.col("FILE_WITH_PATH"), "/").getItem(3))
            .withColumn("date", split(f.col("FILE_WITH_PATH"), "/").getItem(4))
            .withColumn("hour", split(f.col("FILE_WITH_PATH"), "/").getItem(5))
        )

        df_max_date = df_files_date.withColumn(
            "max_date",
            f.concat(
                f.col("year"),
                f.lit("-"),
                f.col("month"),
                f.lit("-"),
                f.col("date"),
                f.lit("-"),
                f.col("hour"),
            ),
        ).drop("year", "month", "date", "hour")

        max_value = df_max_date.agg({"max_date": "max"}).collect()[0][0]
        df_after_src_date = df_max_date.where(df_max_date["max_date"] == max_value)
        df_after_src_date = df_after_src_date.drop("max_date")

    collected_file_path = df_after_src_date.select("FILE_WITH_PATH").toPandas()
    file_path_list = list(collected_file_path["FILE_WITH_PATH"])
    path_list = []
    for i in file_path_list:
        path = "s3a://" + awsbucket + "/" + i
        path_list.append(path)

    collected_uid = df_after_src_date.select("FILE_UID").toPandas()
    list_of_fileid = list(collected_uid["FILE_UID"])

    return list_of_fileid, path_list


def s3_read_lander(
    spark: Any,
    target_table_name: str,
    SNOWFLAKE_SOURCE_NAME: str,
    table_name: str,
    sfoptionsraw: Any,
    path_list: Sequence[str],
) -> None:
    """Load parquet files from S3, reshape them, and write them to Snowflake.

    Args:
        spark: Active Spark session.
        target_table_name: Destination Snowflake table name.
        SNOWFLAKE_SOURCE_NAME: Spark Snowflake connector format name.
        table_name: Source record type name for logging.
        sfoptionsraw: Snowflake connection options.
        path_list: S3 parquet paths to ingest.
    """
    print("lander raw for table >>>", table_name)
    df_inc = spark.read.parquet(*path_list)
    df = df_inc.drop("hash")
    json_data = prepare_data(df)
    write_into_sf(
        spark, json_data, target_table_name, SNOWFLAKE_SOURCE_NAME, sfoptionsraw
    )
