# EMR 6.12.0 — Required JARs

These JARs are **not included in this repository** due to file size.  
Place them in this directory (`emr-6.12.0/`) before submitting the Spark job, or supply them via the `--jars` flag in your `spark-submit` command.

---

## JAR List

| JAR Filename | Version | Purpose | Download |
|---|---|---|---|
| `spark-snowflake_2.12-2.12.0-spark_3.4.jar` | 2.12.0 (Spark 3.4) | Snowflake Spark connector | [Maven Central](https://mvnrepository.com/artifact/net.snowflake/spark-snowflake) |
| `snowflake-jdbc-3.13.22.jar` | 3.13.22 | Snowflake JDBC driver | [Maven Central](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc) |
| `spark-cassandra-connector-assembly_2.12-3.4.0.jar` | 3.4.0 (Scala 2.12) | Apache Cassandra Spark connector | [Maven Central](https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector-assembly) |
| `iceberg-spark-runtime-3.4_2.12-1.3.0.jar` | 1.3.0 (Spark 3.4, Scala 2.12) | Apache Iceberg Spark runtime | [Maven Central](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.4) |
| `postgresql-42.7.1.jar` | 42.7.1 | PostgreSQL JDBC driver | [Maven Central](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| `mysql-connector-java-8.0.30.jar` | 8.0.30 | MySQL JDBC driver | [Maven Central](https://mvnrepository.com/artifact/mysql/mysql-connector-java) |
| `sqljdbc42-6.0.8112.jar` | 6.0.8112 | Microsoft SQL Server JDBC driver | [Microsoft Download](https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server) |
| `bundle-2.17.257.jar` | 2.17.257 | AWS SDK bundle (S3, Secrets Manager, Glue) | [Maven Central](https://mvnrepository.com/artifact/software.amazon.awssdk/bundle) |
| `url-connection-client-2.17.257.jar` | 2.17.257 | AWS SDK URL connection client | [Maven Central](https://mvnrepository.com/artifact/software.amazon.awssdk/url-connection-client) |

---

## Usage with spark-submit

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --jars emr-6.12.0/spark-snowflake_2.12-2.12.0-spark_3.4.jar,\
emr-6.12.0/snowflake-jdbc-3.13.22.jar,\
emr-6.12.0/spark-cassandra-connector-assembly_2.12-3.4.0.jar,\
emr-6.12.0/iceberg-spark-runtime-3.4_2.12-1.3.0.jar,\
emr-6.12.0/postgresql-42.7.1.jar,\
emr-6.12.0/mysql-connector-java-8.0.30.jar,\
emr-6.12.0/sqljdbc42-6.0.8112.jar,\
emr-6.12.0/bundle-2.17.257.jar,\
emr-6.12.0/url-connection-client-2.17.257.jar \
  --py-files src/bronze_lib.py,src/bronze_connector.py,src/bronze_core.py,src/silver_raw_ingest.py,src/grnd_stn_util.py \
  src/bronze_ingest.py \
  '<runtime-config-json>' \
  <sf-secret-name> \
  <sf-database> \
  <sf-warehouse> \
  <sf-role> \
  <sf-url>
```

---

## Notes

- All JARs must be compatible with **Spark 3.4** and **Scala 2.12** (the versions bundled with EMR 6.12.0).
- The bootstrap script `install_python_modules_6_12_0.sh` installs the required Python packages on each EMR node.
- On EMR, JARs can also be distributed via S3 using the `--jars s3://your-bucket/jars/...` syntax.
