"""
This AWS Glue PySpark script reads SCD2-processed data from the refined layer in S3,
filters active records, enriches them with timezone-aware timestamps (UTC and Europe/Berlin),
adds derived columns including DST flag and partition metadata, and writes the result to
the Gold Layer in S3 as partitioned Parquet files for Athena and BI consumption.
"""

import sys
import json
import boto3
import logging
import pytz
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, from_utc_timestamp,
    year, month, dayofmonth, hour, udf,
    current_date
)
from pyspark.sql.types import BooleanType


def is_dst_localized(ts_str):
    """Determine if a timestamp is in DST for Europe/Berlin"""
    tz = pytz.timezone("Europe/Berlin")
    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    return tz.localize(ts, is_dst=None).dst().total_seconds() != 0


is_dst_udf = udf(is_dst_localized, BooleanType())


def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Start Spark
    sc = SparkContext()
    spark = SparkSession(sc)

    # Parse arguments
    args = {k: v for k, v in zip(sys.argv[1::2], sys.argv[2::2])}
    config_bucket = args.get("--config-bucket")
    config_key = args.get("--config-key")

    if not config_bucket or not config_key:
        logger.error("Missing required arguments: --config-bucket or --config-key")
        sys.exit(1)

    # Load config from S3
    s3 = boto3.client("s3")
    config = json.loads(
        s3.get_object(Bucket=config_bucket, Key=config_key)['Body'].read().decode("utf-8")
    )

    domain_bucket = config['s3']['domain_bucket']
    refined_prefix = config['s3']['refined_output_prefix']
    gold_prefix = config['s3']['gold_output_prefix']

    refined_path = f"s3://{domain_bucket}/{refined_prefix}"
    gold_path = f"s3://{domain_bucket}/{gold_prefix}"

    logger.info(f"Reading data from refined layer: {refined_path}")
    df = spark.read.parquet(refined_path)

    if df.rdd.isEmpty():
        logger.info("No data available in refined layer.")
        return

    # Filter active records
    df_active = df.filter(col("is_active") == True)

    # Add timestamp columns
    df_active = df_active \
        .withColumn("delivery_hour_utc", to_timestamp("delivery_start_time_utc")) \
        .withColumn("delivery_hour_berlin", from_utc_timestamp("delivery_hour_utc", "Europe/Berlin"))

    # Add derived columns
    enriched_df = df_active \
        .withColumn("is_dst", is_dst_udf(col("delivery_hour_berlin").cast("string"))) \
        .withColumn("year", year("delivery_hour_berlin")) \
        .withColumn("month", month("delivery_hour_berlin")) \
        .withColumn("day", dayofmonth("delivery_hour_berlin")) \
        .withColumn("hour", hour("delivery_hour_berlin")) \
        .withColumn("database_etl_dt", current_date()) \
        .withColumn("price", col("price").cast("double"))

    final_df = enriched_df.select(
        "delivery_hour_utc",
        "delivery_hour_berlin",
        "price",
        "is_dst",
        "year",
        "month",
        "day",
        "hour",
        "database_etl_dt"
    )

    # Write to Gold Layer
    logger.info(f"Writing Gold Layer data to {gold_path}")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    final_df.write.partitionBy("database_etl_dt").mode("overwrite").parquet(gold_path)

    logger.info("Gold layer update completed successfully.")


if __name__ == "__main__":
    main()
