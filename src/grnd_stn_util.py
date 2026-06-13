from bronze_core import *
from bronze_connector import *
from bronze_lib import get_secret
from typing import Any, Mapping

"""
Fn to generate SQL meta table columns
input: source Database name
output: returns SQL query for selecting information_schema.columns
"""


def sql_fetch_meta_table_columns(
    spark: Any,
    namespace: str,
    sfOptionsRaw: Any,
    arflow_json: Mapping[str, Any],
    source: str,
) -> None:
    """Dispatch metadata extraction for the requested source system.

    Args:
        spark: Active Spark session.
        namespace: Source namespace, schema, or database name.
        sfOptionsRaw: Snowflake options passed through the workflow.
        arflow_json: Runtime configuration from orchestration.
        source: Source system name used to select the reader.
    """
    if source.lower() == "cassandra":
        read_cassandra_metadata(spark, namespace, sfOptionsRaw, arflow_json, source)
    if source.lower() == "azuresql":
        read_azure_metadata(spark, namespace, sfOptionsRaw, arflow_json, source)
    if source.lower() == "mongodb":
        read_mongodb_metadata(spark, namespace, sfOptionsRaw, arflow_json, source)
    if source.lower() == "postgresql":
        read_postgres_metadata(spark, namespace, sfOptionsRaw, arflow_json, source)
    if source.lower() == "mysql":
        read_mysql_metadata(spark, namespace, sfOptionsRaw, arflow_json, source)


"""
Main module of Ground Station Utils
input: source Database name
"""


def load_table_columns_meta_info(
    spark: Any,
    namespace: str,
    sfOptionsRaw: Any,
    arflow_json: Mapping[str, Any],
    source: str,
) -> None:
    """Run source-specific metadata extraction for table column discovery.

    Args:
        spark: Active Spark session.
        namespace: Source namespace, schema, or database name.
        sfOptionsRaw: Snowflake options passed through the workflow.
        arflow_json: Runtime configuration from orchestration.
        source: Source system name used to select the reader.
    """
    emptyRDD = spark.sparkContext.emptyRDD()
    sql_fetch_meta_table_columns(spark, namespace, sfOptionsRaw, arflow_json, source)
    print("load_table_columns_meta_info Completed")


def read_cassandra_metadata(
    spark: Any,
    namespace: str,
    sfOptionsRaw: Any,
    airconf: Mapping[str, Any],
    source: str,
) -> Any:
    """Read column metadata from Cassandra's system schema.

    Args:
        spark: Active Spark session.
        namespace: Source keyspace identifier.
        sfOptionsRaw: Snowflake options forwarded by the caller.
        airconf: Runtime configuration from orchestration.
        source: Source system name.

    Returns:
        A Spark dataframe of Cassandra column metadata.
    """
    print("Inside read_cassandra_metadata")
    try:
        data = (
            spark.read.format("org.apache.spark.sql.cassandra")
            .options(table="columns", keyspace="system_schema")
            .load()
        )
        return data
        print(data.show(n=10, truncate=False, vertical=True))
        print(data.printSchema())
        return data
    except Exception as e:
        logging.exception("exception during cassandra table read >>>>>:" + str(e))
        raise e


def read_azure_metadata(
    spark: Any,
    namespace: str,
    sfOptionsRaw: Any,
    airconf: Mapping[str, Any],
    source: str,
) -> Any:
    """Read column metadata from Azure SQL information schema.

    Args:
        spark: Active Spark session.
        namespace: Azure SQL database name.
        sfOptionsRaw: Snowflake options forwarded by the caller.
        airconf: Runtime configuration from orchestration.
        source: Source system name.

    Returns:
        A Spark dataframe of Azure SQL column metadata.
    """

    print("Inside read_azure_metadata")
    azsql_url = azure_connection_url(airconf, namespace)
    secret_name = airconf["azsecret"]
    az_user, az_pass = get_secret(secret_name)
    try:
        data = (
            spark.read.format("jdbc")
            .option("url", azsql_url)
            .option("user", az_user)
            .option("password", az_pass)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("dbtable", "information_schema.columns")
            .load()
        )
        data = data.select(
            "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"
        )
        print("azure data")
        data.show(n=10, truncate=False)
        return data
    except Exception as e:
        logging.exception("ERROR: Reading data from Azure sql :" + str(e))
        raise e


def read_mongodb_metadata(
    spark: Any,
    namespace: str,
    sfOptionsRaw: Any,
    airconf: Mapping[str, Any],
    source: str,
) -> None:
    """Placeholder for MongoDB metadata extraction.

    Args:
        spark: Active Spark session.
        namespace: MongoDB database name.
        sfOptionsRaw: Snowflake options forwarded by the caller.
        airconf: Runtime configuration from orchestration.
        source: Source system name.
    """
    print("Inside read_mongodb_metadata")
    pass


def read_postgres_metadata(
    spark: Any,
    namespace: str,
    sfOptionsRaw: Any,
    airconf: Mapping[str, Any],
    source: str,
) -> Any:
    """Read column metadata from PostgreSQL information schema.

    Args:
        spark: Active Spark session.
        namespace: PostgreSQL schema or database name.
        sfOptionsRaw: Snowflake options forwarded by the caller.
        airconf: Runtime configuration from orchestration.
        source: Source system name.

    Returns:
        A Spark dataframe of PostgreSQL column metadata.
    """
    print("Inside read_postgres_metadata")
    airconf["postgres_schema"] = namespace
    postgres_url = postgres_connection_url(airconf, namespace)
    src_table_name = "columns"
    secret_name = airconf["azsecret"]
    pg_user, pg_pass = get_secret(secret_name)
    query = "select table_catalog, table_schema, table_name, column_name, data_type from information_schema.columns"
    try:
        data = (
            spark.read.format("jdbc")
            .option("url", postgres_url)
            .option("user", pg_user)
            .option("password", pg_pass)
            .option("driver", "org.postgresql.Driver")
            .option("query", query)
            .load()
        )
        print(data.show(n=10, truncate=False, vertical=True))
        print(data.printSchema())
        return data
    except Exception as e:
        logging.exception("exception during read_postgres_metadata >>>>>:" + str(e))
        raise e


def read_mysql_metadata(
    spark: Any,
    namespace: str,
    sfOptionsRaw: Any,
    airconf: Mapping[str, Any],
    source: str,
) -> Any:
    """Read column metadata from MySQL information schema.

    Args:
        spark: Active Spark session.
        namespace: MySQL schema or database name.
        sfOptionsRaw: Snowflake options forwarded by the caller.
        airconf: Runtime configuration from orchestration.
        source: Source system name.

    Returns:
        A Spark dataframe of MySQL column metadata.
    """
    print("Inside read_mysql_metadata")
    mysql_url = mysql_connection_url(mysql_conf)
    secret_name = airconf["mysqlsecret"]
    mysql_user, mysql_pass = get_secret(secret_name)
    query = "select table_catalog, table_schema, table_name, column_name, data_type from information_schema.columns"
    try:
        data = (
            spark.read.format("jdbc")
            .option("url", mysql_url)
            .option("user", mysql_user)
            .option("password", mysql_pass)
            .option("driver", "com.mysql.jdbc.Driver")
            .option("query", query)
            .load()
        )
        print(data.show(n=10, truncate=False, vertical=True))
        print(data.printSchema())
        return data
    except Exception as e:
        logging.exception(" ERROR while reading data from mysql sql :" + str(e))
        raise e
