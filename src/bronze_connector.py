import logging
import pyspark.sql.functions as f
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from bronze_lib import get_secret
from typing import Any, List, Mapping, Tuple


def push_down_query(
    pipeline_cfg_df: Any, table_name: str
) -> Tuple[List[str], List[str], List[str], List[str]]:
    """Parse source checkpoint metadata into field names, types, operators, and values.

    Args:
        pipeline_cfg_df: Metadata dataframe for source-to-target mappings.
        table_name: Source table name to filter.

    Returns:
        Four aligned lists for field names, field data types, comparison operators,
        and checkpoint values.
    """
    temp_df = pipeline_cfg_df.where(
        pipeline_cfg_df["SOURCE_TABLE_NAME"] == table_name
    )

    bronze_df = temp_df.where(f.col("LAYER_TYPE") == "BRONZE")

    df_prc_fld_name = bronze_df.select("CHECKPOINT_FIELD_DEF").toPandas()
    processing_fld_value = list(df_prc_fld_name["CHECKPOINT_FIELD_DEF"])
    src_obj_processing_field_value = processing_fld_value[0]

    src_obj_processing_field_value_lt = src_obj_processing_field_value.split("|")

    df_src_fld_val = bronze_df.select("CHECKPOINT_TO_VALUE").toPandas()
    df_src_fld_val = list(df_src_fld_val["CHECKPOINT_TO_VALUE"])
    df_src_fld_val = df_src_fld_val[0]
    df_src_fld_val_lt = df_src_fld_val.split("|")

    src_obj_fld_name = []
    src_obj_fld_dt_type = []
    src_obj_fld_op = []
    for el in src_obj_processing_field_value_lt:
        src_obj_fld_name.append(el.split("::")[0])
        src_obj_fld_dt_type.append(el.split("::")[1])
        src_obj_fld_op.append(el.split("::")[2])

    return src_obj_fld_name, src_obj_fld_dt_type, src_obj_fld_op, df_src_fld_val_lt


def rchop(string: str, ending: str) -> str:
    """Remove a trailing suffix when present.

    Args:
        string: Input string to trim.
        ending: Trailing substring to remove.

    Returns:
        The trimmed string when the suffix is present, otherwise the original value.
    """
    if string.endswith(ending):
        return string[: -len(ending)]
    return string


def where_clause_formation(pipeline_cfg_df: Any, table_name: str) -> str:
    """Build a SQL predicate from configured checkpoint metadata.

    Args:
        pipeline_cfg_df: Metadata dataframe for source-to-target mappings.
        table_name: Source table name to filter.

    Returns:
        A SQL `WHERE` clause fragment.
    """
    src_obj_fld_name, src_obj_fld_dt_type, src_obj_fld_op, df_src_fld_val_lt = (
        push_down_query(pipeline_cfg_df, table_name)
    )
    where_clause = ""
    for x, y, z, m in zip(
        src_obj_fld_name, src_obj_fld_dt_type, src_obj_fld_op, df_src_fld_val_lt
    ):
        if y == "datetime" or y == "timestamp":
            m = "'" + m + "'"
        if y == "numeric":
            m = m
        if y == "string":
            m = "'" + m + "'"
        if y == "boolean":
            m = "'" + m + "'"

        # print(x,z,m)
        where_clause = where_clause + x + " " + z + " " + m + " and "

    where_clause = where_clause.rstrip()
    logop = "and"

    whereclause = rchop(where_clause, logop)
    print(whereclause)
    return whereclause


def where_clause_formation_cassandra_key_based(
    meta_map_src_trg_obj_df: Any, table_name: str
) -> str:
    """Build a Cassandra-compatible predicate for key-based incremental reads.

    Args:
        meta_map_src_trg_obj_df: Metadata dataframe for source-to-target mappings.
        table_name: Source table name to filter.

    Returns:
        A Cassandra-compatible predicate string.
    """
    src_obj_fld_name, src_obj_fld_dt_type, src_obj_fld_op, df_src_fld_val_lt = (
        push_down_query(meta_map_src_trg_obj_df, table_name)
    )

    if len(src_obj_fld_name) > 1:
        # breaking in year(yyyy), month(mm) and timestamp
        df_src_fld_val_lt_filter_values = [
            df_src_fld_val_lt[0][:4],
            df_src_fld_val_lt[0][5:7],
            df_src_fld_val_lt[0],
        ]
    else:
        df_src_fld_val_lt_filter_values = df_src_fld_val_lt

    where_clause = ""

    for x, y, z, m in zip(
        src_obj_fld_name,
        src_obj_fld_dt_type,
        src_obj_fld_op,
        df_src_fld_val_lt_filter_values,
    ):
        if y == "datetime" or y == "timestamp":
            m = "to_timestamp('" + m + "')"
        if y == "numeric":
            m = m
        if y == "string":
            m = "'" + m + "'"
        if y == "boolean":
            m = "'" + m + "'"

        where_clause = where_clause + x + " " + z + " " + m + " and "

    where_clause = where_clause.rstrip()
    logop = "and"

    whereclause = rchop(where_clause, logop)
    return whereclause


def read_data_cassandra(spark_session: Any, keyspace: str, table: str) -> Any:
    """Read a full dataset from a Cassandra table.

    Args:
        spark_session: Active Spark session.
        keyspace: Cassandra keyspace.
        table: Cassandra table name.

    Returns:
        A Spark dataframe with the table contents.
    """
    try:
        data = (
            spark_session.read.format("org.apache.spark.sql.cassandra")
            .options(table=table, keyspace=keyspace)
            .load()
        )
        return data

    except Exception as e:
        logging.exception("exception during cassandra table read >>>>>:" + str(e))
        raise e


def read_data_cassandra_key_based(
    spark_session: Any,
    airconf: Mapping[str, Any],
    table_name: str,
    meta_map_src_trg_obj_df: Any,
    namespace: str,
) -> Any:
    """Read incremental Cassandra data using configured checkpoint predicates.

    Args:
        spark_session: Active Spark session.
        airconf: Runtime configuration from orchestration.
        table_name: Source table name.
        meta_map_src_trg_obj_df: Metadata dataframe for source-to-target mappings.
        namespace: Source keyspace.

    Returns:
        A Spark dataframe filtered to the incremental slice.
    """

    whereclause = where_clause_formation_cassandra_key_based(
        meta_map_src_trg_obj_df, table_name
    )

    print("whereclause", whereclause)

    try:
        data = (
            spark_session.read.format("org.apache.spark.sql.cassandra")
            .options(table=table_name, keyspace=namespace)
            .load()
            .filter(whereclause)
        )

        return data

    except Exception as e:
        logging.exception(
            "exception during cassandra key based table read >>>>>:" + str(e)
        )
        raise e


def azure_connection_url(airconf: Mapping[str, Any], namespace: str) -> str:
    """Build the Azure SQL JDBC connection string.

    Args:
        airconf: Runtime configuration from orchestration.
        namespace: Azure SQL database name.

    Returns:
        A JDBC connection string.
    """
    host_name = airconf["host_name"]
    port = airconf["port"]
    database = namespace
    azsql_url = (
        "jdbc:sqlserver://" + host_name + ":" + port + ";databaseName=" + database + ";"
    )

    return azsql_url


def read_data_azuresql(
    spark: Any, airconf: Mapping[str, Any], src_table_name: str, namespace: str
) -> Any:
    """Read a full dataset from Azure SQL.

    Args:
        spark: Active Spark session.
        airconf: Runtime configuration from orchestration.
        src_table_name: Azure SQL source table name.
        namespace: Azure SQL database name.

    Returns:
        A Spark dataframe with the table contents.
    """
    azsql_url = azure_connection_url(airconf, namespace)
    print("azsql_url", azsql_url)
    secret_name = airconf["azsecret"]
    region_name = airconf["global_region_name"]
    az_user, az_pass = get_secret(secret_name, region_name)

    try:
        data = (
            spark.read.format("jdbc")
            .option("url", azsql_url)
            .option("dbtable", src_table_name)
            .option("user", az_user)
            .option("password", az_pass)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
        )

        print("azure data")
        data.show(n=10, truncate=False)
        return data
    except Exception as e:
        logging.exception("ERROR: Reading data from Azure sql :" + str(e))
        raise e


def read_azure_data_key_based(
    spark: Any,
    airconf: Mapping[str, Any],
    table_name: str,
    pipeline_cfg_df: Any,
    namespace: str,
) -> Any:
    """Read incremental Azure SQL data using configured checkpoint predicates.

    Args:
        spark: Active Spark session.
        airconf: Runtime configuration from orchestration.
        table_name: Source table name.
        pipeline_cfg_df: Metadata dataframe for source-to-target mappings.
        namespace: Azure SQL database name.

    Returns:
        A Spark dataframe filtered to the incremental slice.
    """

    azsql_url = azure_connection_url(airconf, namespace)

    print("azsql_url>>>>>>>", azsql_url)

    secret_name = airconf["azsecret"]
    region_name = airconf["global_region_name"]
    az_user, az_pass = get_secret(secret_name, region_name)

    whereclause = where_clause_formation(pipeline_cfg_df, table_name)

    print("whereclause", whereclause)

    connectionProperties = {
        "user": az_user,
        "password": az_pass,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    pushdown_query = f"(select * from {table_name} where {whereclause}){table_name}"

    print("pushdown_query", pushdown_query)

    az_df = spark.read.jdbc(
        url=azsql_url, table=pushdown_query, properties=connectionProperties
    )

    print("azure key based data >>>>>")
    az_df.show(n=20, truncate=False)

    return az_df


def postgres_connection_url(postgres_conf: Mapping[str, Any], database: str) -> str:
    """Build the PostgreSQL JDBC connection string.

    Args:
        postgres_conf: Runtime configuration containing host, port, and schema.
        database: PostgreSQL database name.

    Returns:
        A JDBC connection string.
    """
    host_name = postgres_conf["host_name"]
    port = postgres_conf["port"]
    # database = postgres_conf['database']
    schema_name = postgres_conf["postgres_schema"]
    postgresql_url = (
        "jdbc:postgresql://"
        + host_name
        + ":"
        + port
        + "/"
        + database
        + "?currentSchema="
        + schema_name
    )
    return postgresql_url


def read_data_postgresql(
    spark: Any, airconf: Mapping[str, Any], src_table_name: str, namespace: str
) -> Any:
    """Read a full dataset from PostgreSQL.

    Args:
        spark: Active Spark session.
        airconf: Runtime configuration from orchestration.
        src_table_name: PostgreSQL source table name.
        namespace: PostgreSQL database or schema context.

    Returns:
        A Spark dataframe with the table contents.
    """

    postgres_url = postgres_connection_url(airconf, namespace)
    print("postgresql_url>>>>>>>", postgres_url)
    secret_name = airconf["azsecret"]
    region_name = airconf["global_region_name"]
    pg_user, pg_pass = get_secret(secret_name, region_name)

    try:
        data = (
            spark.read.format("jdbc")
            .option("url", postgres_url)
            .option("dbtable", src_table_name)
            .option("user", pg_user)
            .option("password", pg_pass)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        print("postgreSQL data")
        data.show(n=10, truncate=False)
        return data
    except Exception as e:
        logging.exception("ERROR: Reading data from PostgreSQL :" + str(e))
        raise e


def read_postgresql_data_key_based(
    spark: Any,
    airconf: Mapping[str, Any],
    table_name: str,
    pipeline_cfg_df: Any,
    namespace: str,
) -> Any:
    """Read incremental PostgreSQL data using configured checkpoint predicates.

    Args:
        spark: Active Spark session.
        airconf: Runtime configuration from orchestration.
        table_name: Source table name.
        pipeline_cfg_df: Metadata dataframe for source-to-target mappings.
        namespace: PostgreSQL database or schema context.

    Returns:
        A Spark dataframe filtered to the incremental slice.
    """

    postgresql_url = postgres_connection_url(airconf, namespace)

    print("postgresql_url>>>>>>>", postgresql_url)

    secret_name = airconf["azsecret"]
    region_name = airconf["global_region_name"]
    pg_user, pg_pass = get_secret(secret_name, region_name)

    whereclause = where_clause_formation(pipeline_cfg_df, table_name)

    print("whereclause", whereclause)

    pushdown_query = f"select * from {table_name} where {whereclause}"

    print("pushdown_query", pushdown_query)

    try:
        pg_df = (
            spark.read.format("jdbc")
            .option("url", postgresql_url)
            .option("user", pg_user)
            .option("password", pg_pass)
            .option("driver", "org.postgresql.Driver")
            .option("query", pushdown_query)
            .load()
        )

        print("postgreSQL key based data >>>>>")
        pg_df.show(n=20, truncate=False)

    except Exception as e:
        logging.exception("ERROR: Reading key based data from PostgreSQL :" + str(e))
        raise e

    return pg_df


def mongodb_connection_url(airconf: Mapping[str, Any]) -> str:
    """Build the MongoDB Atlas connection string.

    Args:
        airconf: Runtime configuration from orchestration.

    Returns:
        A MongoDB connection URL.
    """
    secret_name = airconf["mongosecret"]
    region_name = airconf["global_region_name"]
    mongo_user, mongo_pass = get_secret(secret_name, region_name)
    cluster_name = airconf["cluster_name"]

    connection_url = (
        "mongodb+srv://" + mongo_user + ":" + mongo_pass + "@" + cluster_name + "/"
    )

    print("connection_url>>>", connection_url)

    return connection_url


def read_mongodb_data(
    spark: Any, airconf: Mapping[str, Any], table_name: str, namespace: str
) -> Any:
    """Read a MongoDB collection and normalize unsupported null-typed fields.

    Args:
        spark: Active Spark session.
        airconf: Runtime configuration from orchestration.
        table_name: MongoDB collection name.
        namespace: MongoDB database name.

    Returns:
        A Spark dataframe with the collection contents.
    """

    try:

        connection_url = mongodb_connection_url(airconf)
        samplesize = int(airconf["samplesize"])

        mgdb_uri = (
            connection_url
            + namespace
            + "."
            + table_name
            + "?"
            + "tls=true&authSource=admin"
        )

        print("mgdb_uri", mgdb_uri)

        mongodb_table_df = (
            spark.read.format("com.mongodb.spark.sql.DefaultSource")
            .option("uri", mgdb_uri)
            .option("spark.mongodb.input.sampleSize", samplesize)
            .load()
        )

        print("mongodb_table_df>>>>>>>>>>")

        mongodb_table_df.show(n=10, truncate=False)

        print("mongodb_table_df source schema")

        mongodb_table_df.printSchema()

        my_schema = list(mongodb_table_df.schema)

        null_cols = []

        # iterate over schema list to filter for NullType columns

        for st in my_schema:
            if "NullType" in str(st.dataType):
                null_cols.append(st)

        # cast null type columns to string (or whatever you'd like)
        for ncol in null_cols:
            mycolname = str(ncol.name)
            mongodb_table_df = mongodb_table_df.withColumn(
                mycolname, mongodb_table_df[mycolname].cast("string")
            )

    except Exception as e:
        print(str(e))
        logging.exception("exception : " + str(e))
        raise e

    return mongodb_table_df


def mysql_connection_url(mysql_conf: Mapping[str, Any], namespace: str) -> str:
    """Build the MySQL JDBC connection string.

    Args:
        mysql_conf: Runtime configuration from orchestration.
        namespace: MySQL database name.

    Returns:
        A JDBC connection string.
    """
    host_name = mysql_conf["host_name"]
    port = mysql_conf["port"]
    database = namespace
    mysql_url = "jdbc:mysql://" + host_name + ":" + port + "/" + database
    return mysql_url


def read_data_mysql(
    spark: Any, airconf: Mapping[str, Any], src_table_name: str, namespace: str
) -> Any:
    """Read a full dataset from MySQL, including region-specific partner filters.

    Args:
        spark: Active Spark session.
        airconf: Runtime configuration from orchestration.
        src_table_name: MySQL source table name.
        namespace: MySQL database name.

    Returns:
        A Spark dataframe with the selected records.
    """
    mysql_url = mysql_connection_url(airconf, namespace)
    print("mysql_url>>>>>>>", mysql_url)

    secret_name = airconf["azsecret"]
    region_name = airconf["global_region_name"]
    mysql_user, mysql_pass = get_secret(secret_name, region_name)
    print("src_table_name: ", src_table_name)
    try:
        if src_table_name in (
            "partner_partner_mappings",
            "partners",
            "partner_id_mappings",
        ):
            if region_name == "us-east-1":
                preference_region = "('NA')"
            elif region_name == "eu-west-1":
                preference_region = "('NA','EU')"
            elif region_name == "ap-southeast-2":
                preference_region = "('AU')"
            else:
                preference_region = ""

            print(f"Preference region is: {preference_region}")

            if src_table_name == "partners":
                whereclause = f"preference_region in {preference_region}"

            if src_table_name in ("partner_partner_mappings", "partner_id_mappings"):
                whereclause = f"partner_id in (select distinct id from partners where preference_region in {preference_region})"

            print("whereclause", whereclause)
            pushdown_query = f"select * from {src_table_name} where {whereclause}"
            print(push_down_query)

            data = (
                spark.read.format("jdbc")
                .option("url", mysql_url)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", mysql_user)
                .option("password", mysql_pass)
                .option("query", pushdown_query)
                .load()
            )
        else:
            data = (
                spark.read.format("jdbc")
                .option("url", mysql_url)
                .option("user", mysql_user)
                .option("password", mysql_pass)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("query", f"select * from {src_table_name}")
                .load()
            )

        print("mysql data")
        data.show(n=10, truncate=False)
        return data
    except Exception as e:
        logging.exception(" ERROR while reading data from mysql sql :" + str(e))
        raise e


def read_data_mysql_key_based(
    spark: Any,
    airconf: Mapping[str, Any],
    src_table_name: str,
    pipeline_cfg_df: Any,
    namespace: str,
) -> Any:
    """Read incremental MySQL data using configured checkpoint predicates.

    Args:
        spark: Active Spark session.
        airconf: Runtime configuration from orchestration.
        src_table_name: Source table name.
        pipeline_cfg_df: Metadata dataframe for source-to-target mappings.
        namespace: MySQL database name.

    Returns:
        A Spark dataframe filtered to the incremental slice.
    """
    mysql_url = mysql_connection_url(airconf, namespace)
    print("mysql_url>>>>>>>", mysql_url)

    secret_name = airconf["azsecret"]
    region_name = airconf["global_region_name"]
    mysql_user, mysql_pass = get_secret(secret_name, region_name)

    whereclause = where_clause_formation(pipeline_cfg_df, src_table_name)
    print("whereclause", whereclause)
    pushdown_query = f"select * from {src_table_name} where {whereclause}"
    print(push_down_query)

    try:
        data = (
            spark.read.format("jdbc")
            .option("url", mysql_url)
            .option("driver", "com.mysql.jdbc.Driver")
            .option("user", mysql_user)
            .option("password", mysql_pass)
            .option("query", pushdown_query)
            .load()
        )

        print("mysql data")
        data.show(n=10, truncate=False)
        return data
    except Exception as e:
        logging.exception(" ERROR while reading data from mysql sql :" + str(e))
        raise e


def df_from_source_pull_based(
    spark: Any,
    source: str,
    namespace: str,
    src_table_name: str,
    airconf: Mapping[str, Any],
    pull_method: str,
    pipeline_cfg_df: Any,
    target_table_name: str,
) -> Any:
    """Dispatch source reads to the correct connector for the configured mode.

    Args:
        spark: Active Spark session.
        source: Source system name.
        namespace: Source namespace, keyspace, or database name.
        src_table_name: Source table or collection name.
        airconf: Runtime configuration from orchestration.
        pull_method: Extraction mode for the current object.
        pipeline_cfg_df: Metadata dataframe for source-to-target mappings.
        target_table_name: Target dataset name used by some source-specific logic.

    Returns:
        A Spark dataframe containing the requested source slice.
    """
    print("Connector module is called")

    print(
        "namespace for source table  {} is {} and pull method  is {} ".format(
            namespace, src_table_name, pull_method
        )
    )
    print("target_table_name", target_table_name)
    print("airconf", airconf)
    source = source.upper()
    if source == "CASSANDRA":
        print("cassandra connector:")

        if pull_method == "FULL_EXTRACT_INCR" or pull_method == "FULL_EXTRACT":
            print("cassandra block: FULL_EXTRACT_INCR or FULL_EXTRACT")
            cassandra_data = read_data_cassandra(spark, namespace, src_table_name)
            return cassandra_data

        elif pull_method == "KEY_INCR_EXTRACT":
            cassandra_data = read_data_cassandra_key_based(
                spark, airconf, src_table_name, pipeline_cfg_df, namespace
            )
            return cassandra_data

    if source == "AZURESQL":
        print("Azure connector:")

        if pull_method == "FULL_EXTRACT_INCR" or pull_method == "FULL_EXTRACT":
            print("Azure block: FULL_EXTRACT_INCR or FULL_EXTRACT")
            azr_sql_data = read_data_azuresql(spark, airconf, src_table_name, namespace)
            return azr_sql_data
        elif pull_method == "KEY_INCR_EXTRACT":
            print("Azure block: KEY_INCR_EXTRACT")
            azure_key_based_df = read_azure_data_key_based(
                spark, airconf, src_table_name, pipeline_cfg_df, namespace
            )

            return azure_key_based_df

    if source == "POSTGRESQL":
        print("PostgreSQL connector:")

        if pull_method == "FULL_EXTRACT_INCR" or pull_method == "FULL_EXTRACT":
            print("PostgreSQL block: FULL_EXTRACT_INCR or FULL_EXTRACT")
            pgrsql_sql_data = read_data_postgresql(
                spark, airconf, src_table_name, namespace
            )
            return pgrsql_sql_data
        elif pull_method == "KEY_INCR_EXTRACT":
            print("PostgreSQL block: KEY_INCR_EXTRACT")
            pgrsql_sql_data = read_postgresql_data_key_based(
                spark, airconf, src_table_name, pipeline_cfg_df, namespace
            )
            return pgrsql_sql_data

    if source == "MONGODB":
        print("MONGODB Connector:")
        region_name = target_table_name.split("-")[1]
        print("region_name", region_name)
        if pull_method == "FULL_EXTRACT_INCR" or pull_method == "FULL_EXTRACT":
            print("MONOGDB block: FULL_EXTRACT_INCR or FULL_EXTRACT")

            mongodb_table_df = read_mongodb_data(
                spark, airconf, src_table_name, namespace
            )
            mongodb_table_df = mongodb_table_df.withColumn(
                "ITB_DB_REGION", lit(region_name)
            )

            return mongodb_table_df

        elif pull_method == "KEY_INCR_EXTRACT":
            pass

    if source == "MYSQL":
        print("MYSQL Connector:")
        if pull_method == "FULL_EXTRACT_INCR" or pull_method == "FULL_EXTRACT":
            print("MYSQL block: FULL_EXTRACT_INCR or FULL_EXTRACT")

            mysql_table_df = read_data_mysql(spark, airconf, src_table_name, namespace)
            return mysql_table_df

        elif pull_method == "KEY_INCR_EXTRACT":
            mysql_table_df = read_data_mysql_key_based(
                spark, airconf, src_table_name, pipeline_cfg_df, namespace
            )
            return mysql_table_df
