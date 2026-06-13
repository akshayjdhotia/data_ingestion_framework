import argparse
import ast
import pyspark
from pyspark.sql import *
from pyspark import StorageLevel
from typing import Any, Mapping, Tuple
from bronze_core import *
from bronze_lib import get_secret
from grnd_stn_util import load_table_columns_meta_info

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

aws_bucket = "aws_bucket"
processing_grp = "processing_grp"
no_of_out_files = "no_of_out_files"
sf_schema = "sf_schema"
cs_keyspace = "cs_keyspace"


def get_conf() -> Tuple[str, str, str, str, str, str]:
    """Parse spark-submit arguments required by the ingestion entrypoint.

    Returns:
        A tuple containing the serialized runtime config, secret name,
        Snowflake URL, raw database, warehouse, and role.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("conf", help="conf")
    parser.add_argument("v_sf_secret", help="v_sf_secret role")
    parser.add_argument("sf_db_raw", help="snowflake database raw")
    parser.add_argument("sf_warehouse", help="snowflake warehouse")
    parser.add_argument("sf_role", help="snowflake schema")
    parser.add_argument("sf_url", help="Snowflake url")

    args, _ = parser.parse_known_args()

    conf = args.conf
    v_sf_secret = args.v_sf_secret
    sfURL = args.sf_url
    sfDatabaseRaw = args.sf_db_raw
    sfWarehouse = args.sf_warehouse
    sfrole = args.sf_role

    return conf, v_sf_secret, sfURL, sfDatabaseRaw, sfWarehouse, sfrole


conf, sf_v_mount_secret, sfURL, sfdatabaseraw, sfWarehouse, sfrole = get_conf()
arflow_json = ast.literal_eval(conf)
print(arflow_json)
region_name = arflow_json["global_region_name"]
sfUser, sfPassword = get_secret(sf_v_mount_secret, region_name)
awsbucket = arflow_json["aws_bucket"]
namespace = arflow_json["cs_keyspace"]
processinggrp = arflow_json["processing_grp"]
noofoutfiles = arflow_json["no_of_out_files"]
noofoutfiles = int(noofoutfiles)
sfSchema = arflow_json["sf_schema"]

sourceName = ""
getMetaTableInfo = ""
if arflow_json.get("get_meta_table_info") == None:
    getMetaTableInfo = "false"
else:
    getMetaTableInfo = arflow_json["get_meta_table_info"]
if arflow_json.get("source_name") == None:
    sourceName = ""
else:
    sourceName = arflow_json["source_name"]

sfOptionsRaw = {
    "sfURL": sfURL,
    "sfUser": sfUser,
    "sfPassword": sfPassword,
    "sfDatabase": sfdatabaseraw,
    "sfSchema": sfSchema,
    "sfWarehouse": sfWarehouse,
    "sfrole": sfrole,
    "truncate_table": "ON",
    "usestagingtable": "OFF",
}


def init_spark_session(arflow_json: Mapping[str, Any], app_name: str) -> Any:
    """Create a Spark session for standard or Iceberg-enabled bronze ingestion.

    Args:
        arflow_json: Runtime configuration passed from orchestration.
        app_name: Spark application name.

    Returns:
        A configured Spark session.
    """

    is_iceberg_enabled = arflow_json.get("is_iceberg_enabled", "false")
    if is_iceberg_enabled.lower() == "true":
        awsbucket = arflow_json["aws_bucket"]
        iceberg_warehouse = f"s3://{awsbucket}/"
        print(
            f"Creating spark session for Iceberg with warehouse location {iceberg_warehouse}"
        )

        return (
            SparkSession.builder.appName(app_name)
            .config(
                "spark.sql.catalog.my_icbg_catalog",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                "spark.sql.catalog.my_icbg_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(
                "spark.sql.catalog.my_icbg_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.hadoop.hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            )
            .getOrCreate()
        )

    else:
        conf_list = []
        if "_conf" in arflow_json.keys():
            for key, value in arflow_json["_conf"].items():
                conf_list.append((key, value))
        config = pyspark.SparkConf().setAll(conf_list)

        spark = SparkSession.builder.config(conf=config).appName(app_name).getOrCreate()

        return spark


print("Starting Spark Session")
spark = init_spark_session(arflow_json, "Bronze")
print("Spark All Conf", spark.sparkContext.getConf().getAll())
print("*" * 80)

if getMetaTableInfo.lower() == "true":
    print("Running load_table_columns_meta_info for ", sourceName)
    load_table_columns_meta_info(
        spark, namespace, sfOptionsRaw, arflow_json, sourceName
    )
else:
    bronze_work(
        spark,
        awsbucket,
        namespace,
        processinggrp,
        noofoutfiles,
        sfOptionsRaw,
        arflow_json,
        sfdatabaseraw,
        sfSchema,
        SNOWFLAKE_SOURCE_NAME,
    )
