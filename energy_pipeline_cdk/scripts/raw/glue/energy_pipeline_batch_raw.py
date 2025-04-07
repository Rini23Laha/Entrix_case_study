"""
ETL Script for Raw Data Processing - AWS S3 to Parquet

This script implements a fault-tolerant and production-grade ETL (Extract, Transform, Load) process
that reads raw CSV data from S3, applies transformation logic, and writes it into partitioned Parquet
format back into S3. It also supports checkpointing to ensure incremental processing.

Modules Used:
- boto3: for S3 interactions (config and checkpoint loading/saving)
- pyspark: for distributed data transformation and processing
- logging: structured application logging
- json, sys, traceback: for config parsing, CLI args, and error diagnostics

Key Functionalities:
1. **Setup Logging**: Logs execution details with timestamps and error context.
2. **Load Config**: Reads JSON configuration from an S3 bucket.
3. **Checkpointing**: Reads and saves a "last processed" timestamp to avoid reprocessing.
4. **Data Ingestion**: Reads raw CSV from S3 using Spark.
5. **Transformation**:
   - Casts ingestion timestamp
   - Consolidates DST-sensitive columns (`hour_3A`, `hour_3B`) into `hour_3`
6. **CDC Filtering**: Applies Change Data Capture by comparing ingestion_time against last checkpoint.
7. **Partitioning and Writing**: Writes output in Parquet format partitioned by `database_etl_dt`.
8. **Error Handling**: Gracefully handles exceptions and logs root causes.

Command Line Arguments:
--config-bucket <bucket> : S3 bucket containing the configuration JSON
--config-key <key>       : Key path to the JSON configuration file in S3

Example Run:
    python etl_raw_ingestion.py --config-bucket my-etl-configs --config-key configs/raw_config.json

Return Codes:
0   = Success
1   = Known ETL failure (e.g., schema mismatch, missing column)
99  = Unhandled failure

Expected Config JSON Structure (S3):
{
  "s3": {
    "domain_bucket": "my-data-bucket",
    "input_prefix": "landing/raw/",
    "raw_output_prefix": "processed/raw/",
    "checkpoint_files": {
      "raw": "checkpoints/raw_checkpoint.json"
    }
  }
}
"""

import sys
import json
import boto3
import logging
import traceback
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, coalesce, date_format
from pyspark.sql.types import DoubleType


class ETLError(Exception):
    """Custom exception for ETL processing errors with error codes"""
    def __init__(self, message, error_code=1):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


def setup_logging():
    """Configure logging with production-grade formatting"""
    log_format = '%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt=date_format)

    for logger_name in ['boto3', 'botocore', 's3transfer', 'urllib3']:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    logger = logging.getLogger('etl-data-processor')
    return logger


def load_config(logger, config_bucket, config_key):
    logger.info(f"Loading config from s3://{config_bucket}/{config_key}")
    try:
        s3 = boto3.client("s3")
        config = json.loads(
            s3.get_object(Bucket=config_bucket, Key=config_key)['Body'].read().decode("utf-8")
        )
        logger.info("Configuration loaded successfully")
        return config, s3
    except Exception as e:
        error_msg = f"Failed to load config: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        raise ETLError(error_msg)


def load_checkpoint(logger, s3, domain_bucket, checkpoint_key):
    try:
        logger.info(f"Loading checkpoint from s3://{domain_bucket}/{checkpoint_key}")
        checkpoint_obj = s3.get_object(Bucket=domain_bucket, Key=checkpoint_key)
        checkpoint_data = json.loads(checkpoint_obj['Body'].read().decode('utf-8'))
        last_processed = checkpoint_data.get("last_processed", "1970-01-01T00:00:00")
        logger.info(f"Checkpoint loaded: {last_processed}")
        return last_processed, True
    except s3.exceptions.NoSuchKey:
        logger.info("No checkpoint found. Performing full load.")
        return "1970-01-01T00:00:00", False
    except Exception as e:
        logger.warning(f"Error loading checkpoint: {str(e)}. Falling back to full load.")
        logger.debug(traceback.format_exc())
        return "1970-01-01T00:00:00", False


def save_checkpoint(logger, s3, domain_bucket, checkpoint_key, max_time):
    try:
        logger.info(f"Saving checkpoint: {max_time}")
        s3.put_object(
            Bucket=domain_bucket,
            Key=checkpoint_key,
            Body=json.dumps({"last_processed": str(max_time)}).encode("utf-8")
        )
        logger.info("Checkpoint saved successfully")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {str(e)}")
        logger.debug(traceback.format_exc())


def read_data(logger, spark, input_path):
    logger.info(f"Reading input data from {input_path}")
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
        logger.info(f"Data loaded: {df.count()} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        error_msg = f"Failed to read input data: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        raise ETLError(error_msg)


def transform_data(logger, df):
    logger.info("Transforming data")
    try:
        if "ingestion_time" not in df.columns:
            raise ETLError("Missing required column 'ingestion_time'")

        df = df.withColumn("ingestion_time", to_timestamp("ingestion_time"))
        logger.info("Converted ingestion_time to timestamp")

        # Correct casting for hour_3
        # df = df.withColumn("hour_3", coalesce(col("hour_3A"), col("hour_3B")).cast(DoubleType())) \
        #       .drop("hour_3A", "hour_3B")
        df = df.withColumn(
                    "hour_3", coalesce(col("hour_3A"), col("hour_3B")).cast("double")
                ).drop("hour_3A", "hour_3B")
        logger.info("Consolidated and cast hour_3 column to double")

        return df
    except Exception as e:
        error_msg = f"Transformation failed: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        raise ETLError(error_msg)


def write_data(logger, df, output_path, write_mode):
    logger.info(f"Writing data to {output_path} (mode={write_mode})")
    try:
        df.write.partitionBy("database_etl_dt").mode(write_mode).parquet(output_path)
        logger.info(f"Wrote {df.count()} rows to {output_path}")
    except Exception as e:
        error_msg = f"Write failed: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        raise ETLError(error_msg)


def main():
    start_time = datetime.now()
    logger = setup_logging()
    logger.info(f"ETL job started at {start_time}")

    try:
        sc = SparkContext()
        spark = SparkSession(sc)

        args = {k: v for k, v in zip(sys.argv[1::2], sys.argv[2::2])}
        config_bucket = args.get("--config-bucket")
        config_key = args.get("--config-key")

        if not config_bucket or not config_key:
            raise ETLError("Missing required arguments: --config-bucket or --config-key")

        config, s3 = load_config(logger, config_bucket, config_key)

        domain_bucket = config['s3']['domain_bucket']
        input_prefix = config['s3']['input_prefix']
        raw_output_prefix = config['s3']['raw_output_prefix']
        checkpoint_key = config['s3']['checkpoint_files']['raw']

        last_processed, is_incremental = load_checkpoint(logger, s3, domain_bucket, checkpoint_key)

        input_path = f"s3://{domain_bucket}/{input_prefix}"
        df = read_data(logger, spark, input_path)

        if df.rdd.isEmpty():
            logger.info("No new input data. Exiting.")
            return 0

        df = transform_data(logger, df)

        if is_incremental:
            logger.info(f"Applying CDC filter (ingestion_time > {last_processed})")
            df_filtered = df.filter(col("ingestion_time") > last_processed)
        else:
            df_filtered = df

        if df_filtered.rdd.isEmpty():
            logger.info("No new data after CDC filter. Exiting.")
            return 0

        df_partitioned = df_filtered.withColumn("database_etl_dt", date_format(col("ingestion_time"), "yyyy-MM-dd"))
        write_mode = "append" if is_incremental else "overwrite"
        output_path = f"s3://{domain_bucket}/{raw_output_prefix}"

        write_data(logger, df_partitioned, output_path, write_mode)

        try:
            max_time = df_filtered.agg({"ingestion_time": "max"}).collect()[0][0]
            if max_time:
                save_checkpoint(logger, s3, domain_bucket, checkpoint_key, max_time)
        except Exception as e:
            logger.warning(f"Failed to update checkpoint: {str(e)}")

        logger.info(f"ETL job completed successfully in {(datetime.now() - start_time).total_seconds():.2f}s")
        return 0

    except ETLError as e:
        logger.error(f"ETL Error: {e.message}")
        return e.error_code
    except Exception as e:
        logger.error(f"Unhandled Exception: {str(e)}")
        logger.error(traceback.format_exc())
        return 99


if __name__ == "__main__":
    main()
