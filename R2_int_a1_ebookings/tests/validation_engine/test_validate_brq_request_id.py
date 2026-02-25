from unittest import mock
import boto3
import os
import json
import io
import sys

sys.path.append(
    os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
)  # project root folder
from tests.mock_boto import mock_client_generator, mock_lambda_simple_return

MOCK_ENV = {
    "SEIL_AWS_REGION": "TEST_VAL",
    "EBOOKINGS_S3_FILEIN_BUCKET": "FILE_in_Bucket",
    "EBOOKINGS_S3_ERROR_BUCKET": "ERR_Bucket",
    "SALESFORCE_ADAPTOR": "mock_daptor",
    "EBOOKINGS_S3_TEMP_BUCKET": "TEMP_Bucket",
}

SF_ADAPTOR_OUTPUT = {
    "totalSize": 1,
    "done": True,
    "records": [
        {
            "attributes": {
                "type": "Account",
                "url": "/services/data/v58.0/sobjects/Account/0019r00000LB5AnAAL",
            },
            "StageName": "Campaign Booked",
            "Id": "0019r00000LB5AnAAL",
            "AccountId": "23j12k",
            "SWM_Opportunity_Brief_ID__c": "BriefID",
        }
    ],
}

SF_ADAPTOR_OUTPUT2 = {
    "totalSize": 1,
    "done": True,
    "records": [
        {
            "attributes": {
                "type": "Account",
                "url": "/services/data/v58.0/sobjects/Account/0019r00000LB5AnAAL",
            },
            "Opportunity__c": "",
            "Status__c": "",
            "Id": "0019r00000LB5AnAAL",
            "Type": "Direct Client",
        }
    ],
}


@mock.patch.dict("os.environ", MOCK_ENV, clear=True)
class TestValidateNetworkId:
    def test_lambda_handler(self):

        class MockLambdaClient:
            def __init__(mock_self, *args) -> None:
                pass

            def invoke(mock_self, FunctionName, Payload, **args):
                self.written_content = Payload
                return {
                    "Payload": io.BytesIO(json.dumps(SF_ADAPTOR_OUTPUT).encode("utf=8"))
                }

        class MockLambdaClient2:
            def __init__(mock_self, *args) -> None:
                pass

            def invoke(mock_self, FunctionName, Payload, **args):
                self.written_content = Payload
                return {
                    "Payload": io.BytesIO(
                        json.dumps(SF_ADAPTOR_OUTPUT2).encode("utf=8")
                    )
                }

        class MockCommonUtils:
            def __init__(mock_self, event):
                pass

            def move_file_s3_to_s3(mock_self, from_bucket, to_bucket, file_in_from_bucket, key, brq_file_name):
                pass

            def send_email_notification(mock_self, k1, k2, k3, k4, k5):
                pass

        with mock.patch(
            "functions.common_utils.CommonUtils", MockCommonUtils
        ), mock.patch(
            "boto3.client", mock_client_generator({"lambda": MockLambdaClient})
        ):
            from functions.validation_engine.validate_brq_request_id import (
                lambda_handler,
            )

            event = {
                "brqJsonPath": "json_path",
                "brqId": "brq_id",
                "brqEmail": "brq_email",
                "brqDirPath": "dir_path",
                "brqFileName": "brq_file_name",
                "validationResult": {"continueValidation": True, "details": []},
            }
            response = lambda_handler(event, None)
            assert response["validationResult"]["continueValidation"] == False
