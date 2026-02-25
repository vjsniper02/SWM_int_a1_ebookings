from unittest import mock
import boto3
import os
import json
import io
import tempfile
import shutil

import sys

sys.path.append(
    os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
)  # project root folder


# from functions.a1_2.get_brq_file import GetBRQFileHandler
from functions.a1_2.get_brq_file import GetBRQFileHandler, lambda_handler
from tests.mock_boto import mock_client_generator, mock_lambda_simple_return
from functions.BRQParser import BRQParser

BRQ_FILE_PATH = (
    os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    + "/tests/a1_2/BRQ_error.txt"
)

MOCK_ENV = {
    "EBOOKINGS_S3_FILEIN_BUCKET": "in-bucket",
    "EBOOKINGS_S3_TEMP_BUCKET": "temp-bucket",
    "EBOOKING_SPOT_PROCESSING_STATEMACHINE": "ebooking-spot-processing-statemachine",
    "LQS_PUBLISHER_FUNCTION": "lqs-publisher-function",
    "SEIL_CONFIG_BUCKET_NAME": "seil-config-bucket",
    "SALESFORCE_ADAPTOR": "salesforce-mock-adaptor",
    "SEIL_AWS_REGION": "ap-southeast-2",
    "SEIL_SALES_AREA_MAPPING_PATH": "s3://temp-bucket/sales_area_mapping.csv",
    "LMK_DAYPART_ID": "a1/daypartid/path",
}

SALESFORCE_ADAPTOR_MOCK_RETURN_GET_BRQ_FILE_LIST = """
{
    "done": true,
    "records": [
        {
            "CreatedById": "005Bm000003Xxh7IAC",
            "CreatedDate": "2023-12-12T13:41:40.000+0000",
            "Id": "a91Bm00000020yPIAQ",
            "IsDeleted": false,
            "LastActivityDate": null,
            "LastModifiedById": "005Bm000003Xxh7IAC",
            "LastModifiedDate": "2023-12-12T13:41:40.000+0000",
            "LastReferencedDate": null,
            "LastViewedDate": null,
            "NEILON__Access_Type__c": "Public",
            "NEILON__Account__c": null,
            "NEILON__Allow_To_Delete__c": false,
            "NEILON__Allow_To_Rename__c": true,
            "NEILON__Allow_to_Copy_Move__c": true,
            "NEILON__Allow_to_Download_by_Presigned_URL__c": true,
            "NEILON__Allow_to_Email__c": true,
            "NEILON__Amazon_File_Key__c": "Opportunities/test.brq",
            "NEILON__Bucket_Name__c": "swm-code7-r2-functest-creative",
            "NEILON__Bucket_Region__c": "ap-southeast-2",
            "NEILON__Extension__c": ".brq",
            "NEILON__Opportunity__c": "006Bm000009ypDmIAI",
            "Name": "test.brq",
            "OwnerId": "005Bm000003Xxh7IAC",
            "SystemModstamp": "2023-12-12T13:41:40.000+0000",
            "attributes": {
                "type": "NEILON__File__c",
                "url": "/services/data/v58.0/sobjects/NEILON__File__c/a91Bm00000020yPIAQ"
            }
        }
    ],
    "totalSize": 1
}"""


@mock.patch.dict("os.environ", MOCK_ENV, clear=True)
class TestLocalA12Preprocessing:
    temp_folder = os.path.dirname(__file__) + "/._temp_bucket"

    @classmethod
    def setup_class():
        try:
            shutil.rmtree(TestLocalA12Preprocessing.temp_folder)
        except:
            # pass

            try:
                os.makedirs(TestLocalA12Preprocessing.temp_folder)
            except:
                print(TestLocalA12Preprocessing.temp_folder + " already exists")

        shutil.copyfile(
            os.path.dirname(__file__) + "/sales_area_mapping.csv",
            TestLocalA12Preprocessing.temp_folder + "/sales_area_mapping.csv",
        )

    @classmethod
    def teardown_class():
        # shutil.rmtree(TestLocalA12Preprocessing.temp_folder)
        pass

    def test_lambda_handler(self):

        class MockS3Client:
            def __init__(mock_self, region_name="") -> None:
                pass

            def put_object(mock_self, Bucket, Key, Body):
                file_name = os.path.basename(Key)
                f = open(TestLocalA12Preprocessing.temp_folder + "/" + file_name, "w")
                if type(Body) == str:
                    f.write(Body)
                else:
                    f.write(Body.decode("utf-8"))
                f.close()

            def get_object(mock_self, Bucket, Key):
                file_name = os.path.basename(Key)
                f = open(TestLocalA12Preprocessing.temp_folder + "/" + file_name)
                return {"Body": io.BytesIO(f.read().encode("utf-8"))}

        class SSMMockClient:
            def __init__(self, region_name=""):
                pass

            def get_parameter(self, Name, WithDecryption=True):
                if Name == MOCK_ENV["LMK_DAYPART_ID"]:
                    mock_object = mock.MagicMock()
                    mock_object.Value = "35"
                    return {"Parameter": {"Value": "35"}}
                else:
                    raise NotImplementedError()

        class MockLambdaClient:
            def __init__(mock_self, region_name="") -> None:
                pass

            def invoke(mock_self, FunctionName, InvocationType, Payload):
                json_payload = json.loads(Payload)
                if (
                    "invocationType" in json_payload
                    and json_payload["invocationType"] == "QUERY"
                ):
                    return {
                        "Payload": io.BytesIO(
                            SALESFORCE_ADAPTOR_MOCK_RETURN_GET_BRQ_FILE_LIST.encode(
                                "utf-8"
                            )
                        )
                    }
                elif (
                    "invocationType" in json_payload
                    and json_payload["invocationType"] == "S3LINK_READFILECONTENT"
                ):
                    return {
                        "Payload": io.BytesIO(
                            open(BRQ_FILE_PATH).read().encode("utf-8")
                        )
                    }
                elif FunctionName == MOCK_ENV["LQS_PUBLISHER_FUNCTION"]:
                    f = open(
                        TestLocalA12Preprocessing.temp_folder + "/msg_push_to_lqs.json",
                        "w",
                    )
                    f.write(Payload)
                    f.close()

        GOOD_EVENT = {
            "id": "test-id",
            "detail": {
                "sf_payload": {
                    "sf": {"opportunityID": "opportunity-id"},
                    "campaigns": [{"salesAreaOnCampaigns": []}],
                },
                "campaign_code": 123,
            },
        }

        with mock.patch(
            "boto3.client",
            mock_client_generator(
                {
                    "s3": MockS3Client,
                    "lambda": MockLambdaClient,
                    "ssm": SSMMockClient,
                }
            ),
        ):
            # parse BRQ
            event = json.loads(json.dumps(GOOD_EVENT))
            from functions.a1_2.get_brq_file import (
                lambda_handler as get_brq_file_lambda_handler,
            )

            event = get_brq_file_lambda_handler(event, {})

            # prepare campaign header
            from functions.a1_2.prepare_campaign_header_payload import (
                lambda_handler as prepare_campaign_header_payload_handler,
            )

            event = prepare_campaign_header_payload_handler(event, {})

            # prepare spot payload
            from functions.a1_2.prepare_spot_payload import (
                lambda_handler as prepare_spot_payload_handler,
            )

            event = prepare_spot_payload_handler(event, {})

            # push to LQS
            from functions.a1_2.push_to_lqs import lambda_handler as push_to_lqs_handler

            event = push_to_lqs_handler(event, {})
