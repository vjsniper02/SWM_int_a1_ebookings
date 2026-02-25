from unittest import mock
import boto3
import os
import json
import io
from datetime import datetime

import sys

sys.path.append(
    os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
)  # project root folder

from tests.mock_boto import mock_client_generator, mock_lambda_simple_return
from functions.a1_2.upload_report_to_task_opportunity import UploadReportHandler


MOCK_ENV = {
    "EBOOKINGS_S3_FILEIN_BUCKET": "in-bucket",
    "EBOOKINGS_S3_TEMP_BUCKET": "temp-bucket",
    "SEIL_CONFIG_BUCKET_NAME": "seil-config-bucket",
    "SALESFORCE_ADAPTOR": "salesforce-mock-adaptor",
    "SEIL_AWS_REGION": "ap-southeast-2",
}


GOOD_EVENT = {
    "version": "0",
    "id": "71f18a84-7c6a-cd0e-6f53-8ddc2c10983d",
    "detail-type": "seil.landmark.campaign.created",
    "source": "seil.landmark.campaign.created",
    "account": "333875979924",
    "time": "2024-01-18T19:53:19Z",
    "region": "ap-southeast-2",
    "resources": [],
    "detail": {
        "sf_payload": {
            "sf": {
                "opportunityID": "OpportunityID123",
                "RecordTypeDeveloperName": "EBooking",
                "BRQRequestID": "Request123",
            }
        }
    },
    "spotPrebookingResponsePath": "s3://temp-bucket/71f18a84-7c6a-cd0e-6f53-8ddc2c10983d/spots_repsonse.json",
    "brqJsonBucket": "temp-bucket",
    "brqJsonKey": "brq.json",
    "brqFileName": "test.brq",
    "brqRequestID": "",
    "spotPrebookingReport": "s3://temp-bucket/71f18a84-7c6a-cd0e-6f53-8ddc2c10983d/spots_report.csv",
    "spotPrebookingResultStatus": "Partial",
    "salesforceTaskID": "Task123",
}

UPLOAD_SUCCESS_JSON = json.loads(
    """
{
  "status": "OK",
  "sObjects": [
    {
      "attributes": {
        "type": "NEILON__File__c"
      },
      "NEILON__Public_On_Amazon__c": false,
      "NEILON__Allow_to_Download_by_Presigned_URL__c": true,
      "NEILON__Last_Replaced_By__c": "0058w000000KmqSAAS",
      "NEILON__Access_Type__c": "Public",
      "NEILON__Allow_To_Delete__c": false,
      "NEILON__Allow_To_Rename__c": true,
      "NEILON__Allow_to_Copy_Move__c": true,
      "NEILON__Allow_to_Email__c": true,
      "NEILON__Bucket_Region__c": "ap-southeast-2",
      "NEILON__Category__c": "E-Booking Request",
      "NEILON__Content_Encoding__c": "blob",
      "NEILON__Is_Create_Folders__c": false,
      "NEILON__Presigned_URL_Frequency__c": "Every Week",
      "NEILON__Priority__c": "Highest",
      "NEILON__System_Update__c": false,
      "NEILON__Track_Download_History__c": true,
      "NEILON__Upload_Status__c": "Partial Failed",
      "NEILON__Content_Type__c": "application/binary",
      "Name": "test-2.csv",
      "NEILON__Folder__c": "a93Bm0000002o7BIAQ",
      "NEILON__Extension__c": ".csv",
      "NEILON__Size__c": 8319,
      "NEILON__Bucket_Name__c": "swm-code7-r2-functest-creative",
      "NEILON__Parent_Id__c": "006Bm000008RAptIAG",
      "NEILON__Parent_Object_API_Name__c": "NEILON__Opportunity__c",
      "NEILON__Opportunity__c": "006Bm000008RAptIAG",
      "NEILON__Description__c": "Invalid File extension please upload the valid file."
    }
  ],
  "message": null,
  "data": "",
  "code": "200"
}
"""
)


@mock.patch.dict("os.environ", MOCK_ENV, clear=True)
class TestCreateTaskUpdateOpportunityHandler:

    def test_handle(self):
        self.invoked_payloads = []

        class MockLambdaClient:
            def __init__(mock_self, *args) -> None:
                pass

            def invoke(mock_self, FunctionName, Payload, **args):
                self.invoked_payloads.append(json.loads(Payload))
                return {
                    "Payload": io.BytesIO(
                        json.dumps(UPLOAD_SUCCESS_JSON).encode("utf=8")
                    )
                }

        with mock.patch(
            "boto3.client", mock_client_generator({"lambda": MockLambdaClient})
        ):

            handler = UploadReportHandler(GOOD_EVENT)
            result = handler.handle()
            assert self.invoked_payloads == [
                {
                    "invocationType": "UPLOADFILE",
                    "record_id": "Task123",
                    "file_uri": "s3://temp-bucket/71f18a84-7c6a-cd0e-6f53-8ddc2c10983d/spots_report.csv",
                    "extra_header": {
                        "filename": "spots_report.csv",
                        "NEILON__Category__c": "Other",  # TODO Change to "E-Booking Request" after the ticket R2DEV-2225 fixed
                    },
                },
                {
                    "invocationType": "UPLOADFILE",
                    "record_id": "OpportunityID123",
                    "file_uri": "s3://temp-bucket/71f18a84-7c6a-cd0e-6f53-8ddc2c10983d/spots_report.csv",
                    "extra_header": {
                        "filename": "spots_report.csv",
                        "NEILON__Category__c": "Other",  # TODO Change to "E-Booking Request" after the ticket R2DEV-2225 fixed
                    },
                },
            ]
            assert "uploadFileToTaskResponse" in result
            assert "uploadFileToOpportunityResponse" in result

            assert result["uploadFileToTaskResponse"] == UPLOAD_SUCCESS_JSON
            assert result["uploadFileToOpportunityResponse"] == UPLOAD_SUCCESS_JSON
