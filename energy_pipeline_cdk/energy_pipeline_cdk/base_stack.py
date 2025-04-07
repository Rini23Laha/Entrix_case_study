from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_glue as glue,
    CfnOutput
)
from constructs import Construct


class EnergyPipelineBaseStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # === S3 Buckets ===
        self.landing_bucket = s3.Bucket(self, "LandingBucket")
        self.api_landing_bucket = s3.Bucket(self, "ApiLandingBucket")
        self.domain_bucket = s3.Bucket(self, "DomainEnergyPipelineBucket")

        # === IAM Role for Glue ===
        self.glue_role = iam.Role(self, "GlueServiceRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        # === IAM Role for Quicksight ===
        quicksight_role = iam.Role(self, "QuickSightS3AccessRole",
            assumed_by=iam.ServicePrincipal("quicksight.amazonaws.com")
        )
        quicksight_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3ReadOnlyAccess"))
        self.domain_bucket.grant_read(quicksight_role)

        # === Glue Databases ===
        raw_db = glue.CfnDatabase(self, "RawDb",
            catalog_id=self.account,
            database_input={"name": "energy_raw_db"}
        )

        refined_db = glue.CfnDatabase(self, "RefinedDb",
            catalog_id=self.account,
            database_input={"name": "energy_refined_db"}
        )

        gold_db = glue.CfnDatabase(self, "GoldDb",
            catalog_id=self.account,
            database_input={"name": "energy_gold_db"}
        )

        # === Glue Table (Raw Layer) ===
        glue.CfnTable(self, "AthenaRawTable",
            catalog_id=self.account,
            database_name="energy_raw_db",
            table_input={
                "name": "tb_mkt_price_raw",
                "tableType": "EXTERNAL_TABLE",
                "parameters": {
                    "classification": "parquet"
                },
                "storageDescriptor": {
                    "columns": [
                        {"name": "delivery_day", "type": "string", "comment": "Date in Europe/Berlin timezone (format: YYYY-MM-DD)"},
                        {"name": "ingestion_time", "type": "timestamp", "comment": "Ingestion timestamp in UTC"},
                        *[
                            {"name": f"hour_{i}", "type": "double", "comment": f"Price for delivery hour {i}"}
                            for i in range(1, 25) if i != 3
                        ],
                        {"name": "hour_3", "type": "double", "comment": "Consolidated hour 3 (from hour_3A or hour_3B)"}
                    ],
                    "location": f"s3://{self.domain_bucket.bucket_name}/raw/data/",
                    "inputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "outputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "serdeInfo": {
                        "serializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "parameters": {"serialization.format": "1"}
                    }
                },
                "partitionKeys": [
                    {"name": "database_etl_dt", "type": "string"}
                ]
            }
        ).add_dependency(raw_db)

        # === Glue Table (Refined Layer) ===
        glue.CfnTable(self, "AthenaRefinedTable",
        catalog_id=self.account,
        database_name="energy_refined_db",
        table_input={
            "name": "tb_mkt_price_refined",
            "tableType": "EXTERNAL_TABLE",
            "parameters": {
                "classification": "parquet"
            },
            "storageDescriptor": {
                "columns": [
                    {"name": "delivery_day", "type": "string", "comment": "Date in Europe/Berlin timezone"},
                    {"name": "delivery_start_time_utc", "type": "timestamp", "comment": "UTC timestamp for start of delivery hour"},
                    {"name": "delivery_hour", "type": "int", "comment": "Hour of delivery (1-24)"},
                    {"name": "price", "type": "double", "comment": "Market price for the delivery hour"},
                    {"name": "ingestion_time", "type": "timestamp", "comment": "Timestamp of ingestion"},
                    {"name": "record_start_time", "type": "timestamp", "comment": "SCD2 record valid start timestamp"},
                    {"name": "record_end_time", "type": "timestamp", "comment": "SCD2 record valid end timestamp"},
                    {"name": "is_active", "type": "boolean", "comment": "Indicates if the record is active"},
                    {"name": "hash_val", "type": "string", "comment": "Hash of key fields for SCD2 change detection"}
                ],
                "location": f"s3://{self.domain_bucket.bucket_name}/refined/data",
                "inputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "outputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "serdeInfo": {
                    "serializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "parameters": {"serialization.format": "1"}
                }
            },
            "partitionKeys": [
                {"name": "database_etl_dt", "type": "string"}
            ]
        }
    ).add_dependency(refined_db)

        # === Glue Table (Gold Layer) ===
        
        glue.CfnTable(self, "AthenaGoldTable",
            catalog_id=self.account,
            database_name="energy_gold_db",
            table_input={
                "name": "tb_mkt_price_gold",
                "tableType": "EXTERNAL_TABLE",
                "parameters": {
                    "classification": "parquet"
                },
                "storageDescriptor": {
                    "columns": [
                        {"name": "delivery_hour_utc", "type": "timestamp", "comment": "UTC timestamp for the start of the delivery hour"},
                        {"name": "delivery_hour_berlin", "type": "timestamp", "comment": "Europe/Berlin timestamp for the start of the delivery hour"},
                        {"name": "price", "type": "double", "comment": "Market price for the delivery hour"},
                        {"name": "is_dst", "type": "boolean", "comment": "Indicates if the hour is during daylight saving time"},
                        {"name": "year", "type": "int", "comment": "Year of the delivery hour"},
                        {"name": "month", "type": "int", "comment": "Month of the delivery hour"},
                        {"name": "day", "type": "int", "comment": "Day of the delivery hour"},
                        {"name": "hour", "type": "int", "comment": "Hour of the delivery hour (0-23)"}
                    ],
                    "location": f"s3://{self.domain_bucket.bucket_name}/gold/data",
                    "inputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "outputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "serdeInfo": {
                        "serializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "parameters": {"serialization.format": "1"}
                    }
                },
                "partitionKeys": [
                    {"name": "database_etl_dt", "type": "string"}
                ]
            }
        ).add_dependency(gold_db)


        # === Outputs ===
        CfnOutput(self, "LandingBucketNameOutput", value=self.landing_bucket.bucket_name)
        CfnOutput(self, "ApiLandingBucketNameOutput", value=self.api_landing_bucket.bucket_name)
        CfnOutput(self, "DomainBucketNameOutput", value=self.domain_bucket.bucket_name)
        CfnOutput(self, "GlueRoleArnOutput", value=self.glue_role.role_arn)
        CfnOutput(self, "QuickSightRoleArnOutput", value=quicksight_role.role_arn)
