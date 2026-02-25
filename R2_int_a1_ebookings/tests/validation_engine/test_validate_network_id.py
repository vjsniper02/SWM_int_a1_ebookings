from unittest import mock
import boto3
import os
import json
import io
import sys

sys.path.append(
    os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
)  # project root folder

MOCK_ENV = {
    "SEIL_AWS_REGION": "TEST_VAL",
    "EBOOKINGS_S3_FILEIN_BUCKET": "FILE_in_Bucket",
    "EBOOKINGS_S3_ERROR_BUCKET": "ERR_Bucket",
    "EBOOKINGS_S3_TEMP_BUCKET": "TEMP_Bucket",
}


@mock.patch.dict("os.environ", MOCK_ENV, clear=True)
class TestValidateNetworkId:
    def test_lambda_handler(self):
        class MockS3Resource:
            def Object(mock_self, bucket, key):
                if key == "error.json":
                    mock_object = mock.MagicMock()
                    mock_object.get.return_value = {
                        "Body": io.BytesIO(
                            json.dumps(
                                {
                                    "header": {
                                        "NetworkId": "INVALID",
                                    }
                                }
                            ).encode("utf-8")
                        )
                    }
                    return mock_object
                else:
                    mock_object = mock.MagicMock()
                    mock_object.get.return_value = {
                        "Body": io.BytesIO(
                            json.dumps(
                                {
                                    "header": {
                                        "NetworkId": "SEVNET",
                                    }
                                }
                            ).encode("utf-8")
                        )
                    }
                    return mock_object

        class MockCommonUtils:
            def __init__(mock_self, event):
                pass

            def move_file_s3_to_s3(mock_self, from_bucket, to_bucket, file_in_from_bucket, key, brq_file_name):
                pass

            def send_email_notification(mock_self, k1, k2, k3, k4, k5):
                pass

        def mock_resource(name):
            return MockS3Resource()

        with mock.patch(
            "functions.common_utils.CommonUtils", MockCommonUtils
        ), mock.patch("boto3.resource", mock_resource):
            from functions.validation_engine.validate_network_id import lambda_handler

            event = {
                "brqJsonPath": "json_path",
                "brqId": "brq_id",
                "brqEmail": "brq_email",
                "brqDirPath": "dir_path",
                "brqFileName": "brq_file_name",
                "validationResult": {"continueValidation": True, "details": []},
            }
            response = lambda_handler(event, None)
            assert response["validationResult"]["continueValidation"] == True

            event["brqFileName"] = "error"

            response = lambda_handler(event, None)
            assert response["validationResult"]["continueValidation"] == False
