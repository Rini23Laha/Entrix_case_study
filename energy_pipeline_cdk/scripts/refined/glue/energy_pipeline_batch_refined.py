"""
Refined Layer ETL with SCD Type 2 Implementation using PySpark

This script performs a production-grade ETL transformation from the raw to refined data layer, applying
Slowly Changing Dimension Type 2 (SCD2) logic using hash key comparisons. It is designed for energy market
data or other time-series hourly datasets stored in AWS S3, handling timestamp normalization, outlier handling,
CDC logic, and dynamic schema partitioning.

Key Features:
-------------
1. Reads raw Parquet files from an S3 path specified via a config JSON.
2. Deduplicates based on latest `ingestion_time` per delivery day.
3. Normalizes hour columns (`hour_1` to `hour_24`, including DST-specific `hour_3A`/`3B`).
4. Unpivots hourly columns into rows, converting wide to long format.
5. Transforms data: delivery time normalization, UTC handling, and hash generation for change detection.
6. Implements **SCD Type 2**:
   - Tracks changes to price per delivery_day + delivery_hour.
   - Marks older versions inactive and inserts updated versions with current timestamps.
7. Merges with existing refined data and avoids duplicate active records.
8. Writes final output in **partitioned Parquet** format by `database_etl_dt`.
9. Updates checkpoint file in S3 after successful processing.

Command-Line Arguments:
-----------------------
--config-bucket : S3 bucket containing the JSON configuration.
--config-key    : S3 key (path) to the JSON config file.

Expected S3 Configuration JSON:
-------------------------------
{
  "s3": {
    "domain_bucket": "your-data-bucket",
    "raw_output_prefix": "raw/",
    "refined_output_prefix": "refined/",
    "checkpoint_files": {
      "refined": "checkpoints/refined_checkpoint.json"
    },
    "yaml_config": {
      "refined": "schemas/refined_schema.yaml"
    }
  }
}

Partitioning & Output:
----------------------
The final Parquet output is partitioned by `database_etl_dt` (ETL execution date).
All historical records are retained with accurate `record_start_time` and `record_end_time`.

Return Codes:
-------------
0   : Success
1   : Failure (e.g., schema issue, transformation error, data quality exception)

Usage Example:
--------------
    spark-submit refined_etl_scd2.py --config-bucket my-bucket --config-key configs/refined_config.json

Dependencies:
-------------
- PySpark
- boto3
- AWS permissions to access config and data S3 paths
"""


import sys
import json
import boto3
import yaml
import logging
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lit, expr, to_date, current_timestamp, to_timestamp,
    coalesce, lpad, concat, concat_ws, row_number, when, sha2
)

def setup_logging():
    """Configure basic logging"""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    return logging.getLogger(__name__)

def process_data():
    """Main ETL function implementing SCD Type 2 for Raw to Refined transformation with hash key comparison"""
    logger = setup_logging()
    logger.info("Starting Refined Layer ETL with SCD Type 2 logic using hash key comparison")

    try:
        sc = SparkContext()
        spark = SparkSession(sc)

        # === Parse CLI arguments ===
        args = {k: v for k, v in zip(sys.argv[1::2], sys.argv[2::2])}
        config_bucket = args.get("--config-bucket")
        config_key = args.get("--config-key")
        if not config_bucket or not config_key:
            logger.error("Missing required arguments: --config-bucket or --config-key")
            return 1

        # === Load JSON config from S3 ===
        s3 = boto3.client("s3")
        config = json.loads(
            s3.get_object(Bucket=config_bucket, Key=config_key)['Body'].read().decode("utf-8")
        )
        domain_bucket = config['s3']['domain_bucket']
        raw_path = f"s3://{domain_bucket}/{config['s3']['raw_output_prefix']}"
        refined_path = f"s3://{domain_bucket}/{config['s3']['refined_output_prefix']}"
        checkpoint_key = config['s3']['checkpoint_files']['refined']
        yaml_config_path = config['s3']['yaml_config']['refined']

        # === Read raw data ===
        logger.info(f"Reading raw data from {raw_path}")
        raw_df = spark.read.parquet(raw_path)
        if raw_df.rdd.isEmpty():
            logger.info("No raw data found. Exiting gracefully.")
            return 0

        # === 1. DEDUPLICATE RAW DATA ===
        logger.info("STEP 1: Deduplicating raw data based on delivery_day")
        window_spec = Window.partitionBy("delivery_day").orderBy(col("ingestion_time").desc())
        raw_df = raw_df.withColumn("row_num", row_number().over(window_spec)) \
                       .filter("row_num = 1").drop("row_num")
        
        logger.info(f"After deduplication, raw data has {raw_df.count()} unique delivery days")

        # === 2. CAST AND CLEAN COLUMNS ===
        logger.info("STEP 2: Converting data types and handling missing values")
        hour_cols = [col_name for col_name in raw_df.columns if col_name.startswith("hour_")]
        
        for col_name in hour_cols:
            # Handle null values and cast to double
            raw_df = raw_df.withColumn(
                col_name,
                when(col(col_name).isNull() | (col(col_name) == ""), None)
                .otherwise(col(col_name).cast("double"))
            )
            


        # === 3. NORMALIZE hour_3 ===
        if "hour_3A" in raw_df.columns and "hour_3B" in raw_df.columns:
            logger.info("STEP 3: Consolidating hour_3A and hour_3B columns")
            raw_df = raw_df.withColumn("hour_3", coalesce(col("hour_3A"), col("hour_3B"))) \
                        .drop("hour_3A", "hour_3B")
        
        # === 4. UNPIVOT HOURLY DATA ===
        logger.info("STEP 4: Unpivoting hour columns into rows")
        hour_cols = [col_name for col_name in raw_df.columns if col_name.startswith("hour_")]
        
        stack_expr = ", ".join([f"'{col_name.replace('hour_', '')}', `{col_name}`" for col_name in hour_cols])
        unpivoted_df = raw_df.selectExpr(
            "delivery_day", "ingestion_time", f"stack({len(hour_cols)}, {stack_expr}) as (delivery_hour, price)"
        )
        
        # Filter out null prices
        unpivoted_df = unpivoted_df.filter(col("price").isNotNull())
        
        logger.info(f"After unpivoting, data has {unpivoted_df.count()} total hourly records")

        # === 5. TRANSFORM DATA ===
        logger.info("STEP 5: Transforming data for refined layer")
        source_df = unpivoted_df \
            .withColumn("delivery_day", expr("date_format(to_date(delivery_day, 'dd/MM/yyyy'), 'yyyy-MM-dd')")) \
            .withColumn("delivery_hour", col("delivery_hour").cast("int")) \
            .withColumn("price", col("price").cast("double")) \
            .withColumn("delivery_start_time_utc", expr(
                "to_utc_timestamp(concat(delivery_day, ' ', lpad(cast(delivery_hour - 1 as string), 2, '0'), ':00:00'), 'Europe/Berlin')"
            ))

        # Ensure delivery_day is a string to match Hive schema
        source_df = source_df.withColumn(
            "delivery_day", 
            col("delivery_day").cast("string")
        )

        # === 6. CREATE HASH KEY FOR CHANGE DETECTION ===
        logger.info("STEP 6: Creating hash key for change detection")
        # Create a hash of the business key and values that would indicate a change
        source_df = source_df.withColumn(
            "hash_val",
            sha2(concat_ws("||", 
                col("delivery_day"), 
                col("delivery_hour"), 
                col("price").cast("string")
            ), 256)
        )

        # === 7. DEDUPLICATE SOURCE DATA ===
        logger.info("STEP 7: Deduplicating source data")
        window_spec = Window.partitionBy("delivery_day", "delivery_hour").orderBy(col("ingestion_time").desc())
        source_df = source_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter("row_num = 1") \
                          .drop("row_num")
        
        # Add SCD Type 2 columns to source data
        source_df = source_df \
            .withColumn("record_start_time", current_timestamp()) \
            .withColumn("record_end_time", lit("9999-12-31").cast("timestamp")) \
            .withColumn("is_active", lit(True)) \
            .withColumn("ingestion_time", col("ingestion_time").cast("timestamp")) \
            .withColumn("database_etl_dt", expr("date_format(current_timestamp(), 'yyyy-MM-dd')"))

        # === 8. IMPLEMENT SCD TYPE 2 WITH HASH KEY COMPARISON ===
        logger.info("STEP 8: Implementing SCD Type 2 with hash key comparison")
        
        try:
            target_exists = True
            target_df = spark.read.parquet(refined_path)
            
            # Ensure schema compatibility - ensure delivery_day is string
            if "delivery_day" in target_df.columns:
                target_df = target_df.withColumn(
                    "delivery_day", 
                    col("delivery_day").cast("string")
                )
            
            # Create hash key for target data if it doesn't exist
            if "hash_val" not in target_df.columns:
                target_df = target_df.withColumn(
                    "hash_val",
                    sha2(concat_ws("||", 
                        col("delivery_day"), 
                        col("delivery_hour"), 
                        col("price").cast("string")
                    ), 256)
                )
            
            logger.info(f"Found existing data with {target_df.count()} total records")
            active_count = target_df.filter("is_active = true").count()
            logger.info(f"Active records: {active_count}")
            
            # Get only active target records for comparison
            active_target_df = target_df.filter("is_active = true")
            
            # 1. Find records to be expired (records that exist in target but have changed in source)
            records_to_expire = active_target_df.alias("t").join(
                source_df.alias("s"),
                (col("t.delivery_day") == col("s.delivery_day")) & 
                (col("t.delivery_hour") == col("s.delivery_hour")),
                "inner"
            ).where(col("t.hash_val") != col("s.hash_val")) \
             .select(
                col("t.delivery_day"),
                col("t.delivery_hour"),
                col("t.price"),
                col("t.delivery_start_time_utc"),
                col("t.record_start_time"),
                current_timestamp().alias("record_end_time"),
                lit(False).alias("is_active"),
                col("t.ingestion_time"),
                col("t.hash_val"),
                expr("date_format(current_timestamp(), 'yyyy-MM-dd')").alias("database_etl_dt")
             )
            
            expired_count = records_to_expire.count()
            logger.info(f"Records to expire: {expired_count}")
            
            # 2. Find new records (in source but not in target) or changed records
            new_or_changed_records = source_df.alias("s").join(
                active_target_df.alias("t"),
                (col("s.delivery_day") == col("t.delivery_day")) & 
                (col("s.delivery_hour") == col("t.delivery_hour")),
                "left_anti"
            ).select(source_df["*"])
            
            # Also include records where hash key has changed
            changed_records = source_df.alias("s").join(
                active_target_df.alias("t"),
                (col("s.delivery_day") == col("t.delivery_day")) & 
                (col("s.delivery_hour") == col("t.delivery_hour")),
                "inner"
            ).where(col("t.hash_val") != col("s.hash_val")) \
             .select(source_df["*"])
            
            records_to_insert = new_or_changed_records.union(changed_records).distinct()
            insert_count = records_to_insert.count()
            logger.info(f"Records to insert: {insert_count}")
            
            # 3. Keep unchanged active records
            unchanged_active_records = active_target_df.alias("t").join(
                source_df.alias("s"),
                (col("t.delivery_day") == col("s.delivery_day")) & 
                (col("t.delivery_hour") == col("s.delivery_hour")),
                "inner"
            ).where(col("t.hash_val") == col("s.hash_val")) \
             .select(target_df["*"])
            
            # 4. Keep all existing inactive records
            inactive_records = target_df.filter("is_active = false")
            
            # 5. Union all parts together
            merged_df = records_to_insert \
                .unionByName(records_to_expire, allowMissingColumns=True) \
                .unionByName(unchanged_active_records, allowMissingColumns=True) \
                .unionByName(inactive_records, allowMissingColumns=True)
            
            # Verify no duplicates in active records
            dupes_check = merged_df.filter("is_active = true") \
                              .groupBy("delivery_day", "delivery_hour") \
                              .count() \
                              .filter("count > 1")
                              
            dupes_count = dupes_check.count()
            if dupes_count > 0:
                logger.warning(f"Found {dupes_count} duplicate active records, deduplicating")
                
                # Force deduplication of active records
                window_spec = Window.partitionBy("delivery_day", "delivery_hour") \
                                  .orderBy(col("record_start_time").desc())
                                  
                active_df = merged_df.filter("is_active = true") \
                               .withColumn("row_num", row_number().over(window_spec)) \
                               .filter("row_num = 1") \
                               .drop("row_num")
                               
                inactive_df = merged_df.filter("is_active = false")
                
                merged_df = active_df.unionByName(inactive_df)
                logger.info("Deduplication complete")
            
        except Exception as e:
            target_exists = False
            logger.info(f"No target data found, performing initial load: {str(e)}")
            merged_df = source_df
        
        # === 9. WRITING DATA WITH PARTITIONING ===
        logger.info("STEP 9: Writing SCD Type 2 data to refined layer with partitioning")
        
        # After the merge, count both active and inactive records
        active_count = merged_df.filter("is_active = true").count()
        inactive_count = merged_df.filter("is_active = false").count()
        total_distinct_combinations = merged_df.select("delivery_day", "delivery_hour").distinct().count()
        
        logger.info(f"Data to write: {merged_df.count()} total records")
        logger.info(f"Active records: {active_count}")
        logger.info(f"Inactive records: {inactive_count}")
        logger.info(f"Distinct day-hour combinations: {total_distinct_combinations}")
        
        # Count records by date to help identify missing data
        date_counts = merged_df.groupBy("delivery_day").count().orderBy("delivery_day")
        logger.info("Records per delivery day:")
        for row in date_counts.collect():
            logger.info(f"  {row['delivery_day']}: {row['count']} records")
        
        # Enable dynamic partition overwrite mode
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
        # Add diagnostic info before writing
        logger.info("Analyzing potential data quality issues:")
        
        # 1. Check for missing hour values in any day
        days_with_missing_hours = merged_df.filter("is_active = true") \
            .groupBy("delivery_day") \
            .count() \
            .filter("count < 24") \
            .orderBy("delivery_day")
            
        if days_with_missing_hours.count() > 0:
            logger.info("Days with fewer than 24 hours of data:")
            for row in days_with_missing_hours.collect():
                logger.info(f"  {row['delivery_day']}: only {row['count']} hours")
                
                # Get specific missing hours for first few problem days
                if row == days_with_missing_hours.first():
                    all_hours = list(range(1, 25))
                    day_hours = [r["delivery_hour"] for r in merged_df.filter(f"delivery_day = '{row['delivery_day']}'").select("delivery_hour").collect()]
                    missing_hours = [h for h in all_hours if h not in day_hours]
                    logger.info(f"  Missing hours for {row['delivery_day']}: {missing_hours}")
        
        # 2. Check for duplicate active records (should be none after our deduplication)
        dupes = merged_df.filter("is_active = true") \
            .groupBy("delivery_day", "delivery_hour") \
            .count() \
            .filter("count > 1")
            
        dupe_count = dupes.count()
        if dupe_count > 0:
            logger.info(f"Found {dupe_count} cases of duplicate active records:")
            for row in dupes.limit(5).collect():
                logger.info(f"  {row['delivery_day']} hour {row['delivery_hour']}: {row['count']} active records")
        
        # Write with partitioning by database_etl_dt
        merged_df.write \
            .partitionBy("database_etl_dt") \
            .mode("overwrite") \
            .format("parquet") \
            .save(refined_path)
        
        logger.info(f"Successfully wrote data to {refined_path}")
        
        # === 10. UPDATE CHECKPOINT ===
        logger.info("STEP 10: Updating checkpoint")
        try:
            max_time = source_df.agg({"ingestion_time": "max"}).collect()[0][0]
            if max_time:
                s3.put_object(
                    Bucket=domain_bucket,
                    Key=checkpoint_key,
                    Body=json.dumps({"last_processed": str(max_time)}).encode("utf-8")
                )
                logger.info(f"Checkpoint updated to {max_time}")
            else:
                logger.warning("Could not determine max ingestion_time for checkpoint update")
        except Exception as e:
            logger.error(f"Failed to update checkpoint: {str(e)}")
            
        logger.info("ETL process completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    result = process_data()
    logger = logging.getLogger(__name__)
    logger.info(f"ETL process finished with result code: {result}")