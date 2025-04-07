import sys
import os
import json
from io import BytesIO
from unittest import mock
from pyspark.sql import Row

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../..")))
import energy_pipeline_cdk.scripts.data_quality.dq_validation_gold as dq_script


@mock.patch("energy_pipeline_cdk.scripts.data_quality.dq_validation_gold.SparkContext")
@mock.patch("energy_pipeline_cdk.scripts.data_quality.dq_validation_gold.SparkSession")
@mock.patch("energy_pipeline_cdk.scripts.data_quality.dq_validation_gold.boto3.client")
@mock.patch("energy_pipeline_cdk.scripts.data_quality.dq_validation_gold.col")
def test_dq_validation_main(
    mock_col,
    mock_boto_client,
    mock_spark_session_class,
    mock_spark_context
):
    # === Setup Spark mocks ===
    spark_mock = mock.MagicMock()
    mock_spark_session_class.return_value = spark_mock

    df_mock = mock.MagicMock()
    spark_mock.read.parquet.return_value = df_mock

    # === Mock chained filter().count() ===
    bad_price_df = mock.MagicMock()
    invalid_hour_df = mock.MagicMock()

    bad_price_df.count.return_value = 5
    invalid_hour_df.count.return_value = 3

    df_mock.filter.side_effect = [bad_price_df, invalid_hour_df]

    # === Mock groupBy().count().filter().count() ===
    duplicate_df = mock.MagicMock()
    duplicate_df.count.return_value = 2
    df_mock.groupBy.return_value.count.return_value.filter.return_value = duplicate_df

    # === Mock col() expressions ===
    col_expr_mock = mock.MagicMock()
    is_null_mock = mock.MagicMock()
    le_mock = mock.MagicMock()
    or_mock = mock.MagicMock()

    is_null_mock.__or__.return_value = or_mock

    col_expr_mock.isNull.return_value = is_null_mock
    col_expr_mock.__le__.return_value = le_mock
    col_expr_mock.__lt__.return_value = mock.MagicMock()
    col_expr_mock.__gt__.return_value = mock.MagicMock()

    mock_col.side_effect = lambda name: col_expr_mock

    # === Mock boto3 client ===
    s3_mock = mock.MagicMock()
    mock_boto_client.return_value = s3_mock

    s3_mock.get_object.return_value = {
        "Body": BytesIO(json.dumps({
            "s3": {
                "domain_bucket": "test-bucket",
                "gold_output_prefix": "gold/"
            }
        }).encode("utf-8"))
    }
    s3_mock.put_object.return_value = {}

    # === Simulate CLI args ===
    sys.argv = [
        "script.py",
        "--config-bucket", "my-bucket",
        "--config-key", "dq/config.json"
    ]

    # === Run main ===
    dq_script.main()

    # === Validate ===
    s3_mock.put_object.assert_called_once()
    uploaded_json = s3_mock.put_object.call_args[1]["Body"].decode("utf-8")
    report = json.loads(uploaded_json)

    assert report == {
        "bad_price_count": 5,
        "invalid_hour_count": 3,
        "duplicate_count": 2
    }
