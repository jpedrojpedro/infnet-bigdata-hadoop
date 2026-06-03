import argparse
from pyspark.sql import SparkSession


def main():
    # 1. Parse arguments sent by Airflow's SparkSubmitOperator
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-sqlite-path', required=True)
    parser.add_argument('--output-parquet-path', required=True)
    args = parser.parse_args()

    # 2. Initialize Spark
    spark = SparkSession.builder \
        .appName("Airflow-Triggered-ETL") \
        .getOrCreate()

    # 3. Use the parsed input argument for the connection string
    # Expected format passed from Airflow: /opt/spark/jobs/data/northwind.db
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlite:{args.input_sqlite_path}") \
        .option("dbtable", "Orders") \
        .option("driver", "org.sqlite.JDBC") \
        .load()

    # 4. Use the parsed output argument for the destination
    # Expected format passed from Airflow: hdfs://namenode:9000/output/orders_parquet
    df.write \
        .mode("overwrite") \
        .parquet(args.output_parquet_path)

    spark.stop()


if __name__ == "__main__":
    main()
