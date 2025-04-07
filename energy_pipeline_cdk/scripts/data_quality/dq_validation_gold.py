from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import boto3
import json
import sys

def main():
    sc = SparkContext()
    spark = SparkSession(sc)

    args = {k: v for k, v in zip(sys.argv[1::2], sys.argv[2::2])}
    bucket = args.get("--config-bucket")
    key = args.get("--config-key")

    s3 = boto3.client("s3")
    config = json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode("utf-8"))

    path = f"s3://{config['s3']['domain_bucket']}/{config['s3']['gold_output_prefix']}"
    df = spark.read.parquet(path)

    bad_price = df.filter((col("price").isNull()) | (col("price") <= 0))
    null_count = bad_price.count()

    hour_check = df.filter((col("delivery_hour") < 1) | (col("delivery_hour") > 24))
    hour_issue_count = hour_check.count()

    dup_df = df.groupBy("delivery_day", "delivery_hour").count().filter("count > 1")
    dup_count = dup_df.count()

    report = {
        "bad_price_count": null_count,
        "invalid_hour_count": hour_issue_count,
        "duplicate_count": dup_count
    }

    print(json.dumps(report, indent=2))

    s3.put_object(
        Bucket=config['s3']['domain_bucket'],
        Key="dq_reports/dq_report.json",
        Body=json.dumps(report).encode("utf-8")
    )

if __name__ == "__main__":
    main()