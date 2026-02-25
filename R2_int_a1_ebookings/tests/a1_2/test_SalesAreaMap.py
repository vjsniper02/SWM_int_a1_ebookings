from os import path
import io
import os
from unittest import mock
import boto3

import sys

sys.path.append(
    os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
)  # project root folder

from tests.mock_boto import mock_client_generator
from functions.a1_2.SalesAreaMap import SalesAreaMap

TEST_CSV_FILE_NAME = "sales_area_mapping.csv"

MOCK_ENV = {
    "EBOOKINGS_S3_FILEIN_BUCKET": "in-bucket",
    "EBOOKINGS_S3_TEMP_BUCKET": "temp-bucket",
    "SEIL_CONFIG_BUCKET_NAME": "seil-config-bucket",
    "SALESFORCE_ADAPTOR": "salesforce-mock-adaptor",
    "SEIL_SALES_AREA_MAPPING_PARAM_NAME": "sales_area_mapping.csv",
    "SEIL_AWS_REGION": "ap-southeast-2",
}


class TestSalesAreaMap:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_SalesAreaMap_withContent(self):
        with open(
            path.join(path.dirname(__file__), TEST_CSV_FILE_NAME),
            mode="r",
            encoding="utf-8-sig",
        ) as f:
            content = f.read()
        sales_area_map = SalesAreaMap(sales_area_csv=content)
        assert len(sales_area_map.fieldnames) == 22
        assert len(sales_area_map.data) == 144
        assert sales_area_map.data[0]["salesAreaNumber"] == "1"
        assert sales_area_map.data[0]["code"] == "01"

    def test_SalesAreaMap_withPath(self):
        class S3MockClient:
            def __init__(self, region_name=""):
                pass

            def get_object(self, Bucket="", Key=""):
                if (
                    Bucket == MOCK_ENV["SEIL_CONFIG_BUCKET_NAME"]
                    and Key == TEST_CSV_FILE_NAME
                ):
                    with open(
                        path.join(path.dirname(__file__), TEST_CSV_FILE_NAME),
                        mode="r",
                        encoding="utf-8-sig",
                    ) as f:
                        content = f.read()

                    return {"Body": io.BytesIO(content.encode("utf-8"))}
                else:
                    raise NotImplementedError()

        with mock.patch("boto3.client", mock_client_generator({"s3": S3MockClient})):
            sales_area_map = SalesAreaMap(
                sales_area_path=f"s3://{MOCK_ENV['SEIL_CONFIG_BUCKET_NAME']}/{TEST_CSV_FILE_NAME}"
            )
            assert len(sales_area_map.fieldnames) == 22
            assert len(sales_area_map.data) == 144
            assert sales_area_map.data[0]["salesAreaNumber"] == "1"
            assert sales_area_map.data[0]["code"] == "01"

    def test_SalesAreaMap_withParamName(self):
        class S3MockClient:
            def __init__(self, region_name=""):
                pass

            def get_object(self, Bucket="", Key=""):
                if (
                    Bucket == MOCK_ENV["SEIL_CONFIG_BUCKET_NAME"]
                    and Key == TEST_CSV_FILE_NAME
                ):
                    with open(
                        path.join(path.dirname(__file__), TEST_CSV_FILE_NAME),
                        mode="r",
                        encoding="utf-8-sig",
                    ) as f:
                        content = f.read()

                    return {"Body": io.BytesIO(content.encode("utf-8"))}
                else:
                    raise NotImplementedError()

        class SSMMockClient:
            def __init__(self, region_name=""):
                pass

            def get_parameter(self, Name, WithDecryption=True):
                if Name == MOCK_ENV["SEIL_SALES_AREA_MAPPING_PARAM_NAME"]:
                    mock_object = mock.MagicMock()
                    mock_object.Value = f"s3://{MOCK_ENV['SEIL_CONFIG_BUCKET_NAME']}/{TEST_CSV_FILE_NAME}"
                    return {"Parameter": mock_object}
                else:
                    raise NotImplementedError()

        with mock.patch(
            "boto3.client",
            mock_client_generator({"s3": S3MockClient, "ssm": SSMMockClient}),
        ):
            sales_area_map = SalesAreaMap(
                param_name=MOCK_ENV["SEIL_SALES_AREA_MAPPING_PARAM_NAME"]
            )
            assert len(sales_area_map.fieldnames) == 22
            assert len(sales_area_map.data) == 144
            assert sales_area_map.data[0]["salesAreaNumber"] == "1"
            assert sales_area_map.data[0]["code"] == "01"
