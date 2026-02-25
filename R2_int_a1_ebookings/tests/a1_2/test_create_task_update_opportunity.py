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
from functions.a1_2.create_task_update_opportunity import (
    CreateTaskUpdateOpportunityHandler,
)


SF_ADAPTOR_INPUT = {
    "invocationType": "COMPOSITE",
    "payload": {
        "allOrNone": True,
        "compositeRequest": [
            {
                "method": "POST",
                "url": "/services/data/v58.0/sobjects/Task",
                "referenceId": "refTask",
                "body": {
                    "Subject": "Call",
                    "Description": "BRQ spot 'test.brq' has been loaded and the status is 'Partial'",
                    "whatId": "OpportunityID123",
                    "ActivityDate": "2023-10-23",
                    "Type": "Call",
                },
            },
            {
                "method": "PATCH",
                "url": "/services/data/v58.0/sobjects/Opportunity/OpportunityID123",
                "referenceId": "refOpp",
                "body": {"SWM_Spots_created__c": "Partial"},
            },
        ],
    },
}

SF_ADAPTOR_OUTPUT = {
    "compositeResponse": [
        {
            "body": {"id": "TaskID123", "success": True, "errors": []},
            "httpHeaders": {"Location": "/services/data/v58.0/sobjects/Task/TaskID123"},
            "httpStatusCode": 201,
            "referenceId": "refTask",
        },
        {
            "body": None,
            "httpHeaders": {},
            "httpStatusCode": 204,
            "referenceId": "refOpp",
        },
    ]
}


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
    "spotPrebookingReport": "s3://temp-bucket/71f18a84-7c6a-cd0e-6f53-8ddc2c10983d/spots_report.csv",
    "spotPrebookingResultStatus": "Partial",
}


@mock.patch.dict("os.environ", MOCK_ENV, clear=True)
class TestCreateTaskUpdateOpportunityHandler:

    def test_handle(self):

        class MockLambdaClient:
            def __init__(mock_self, *args) -> None:
                pass

            def invoke(mock_self, FunctionName, Payload, **args):
                self.written_content = Payload
                return {
                    "Payload": io.BytesIO(json.dumps(SF_ADAPTOR_OUTPUT).encode("utf=8"))
                }

        with mock.patch(
            "boto3.client", mock_client_generator({"lambda": MockLambdaClient})
        ):
            with mock.patch(
                "functions.a1_2.create_task_update_opportunity.datetime", wrap=datetime
            ) as mock_datetime:
                mock_datetime.now.return_value = datetime(2023, 10, 23, 18, 10, 59)

                handler = CreateTaskUpdateOpportunityHandler(GOOD_EVENT)
                result = handler.handle()
                assert json.loads(self.written_content) == SF_ADAPTOR_INPUT
                assert "salesforceTaskID" in result
                assert result["salesforceTaskID"] == "TaskID123"
