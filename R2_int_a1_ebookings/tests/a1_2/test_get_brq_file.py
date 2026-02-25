from unittest import mock
import boto3
import os
import json
import io

import sys

sys.path.append(
    os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
)  # project root folder


# from functions.a1_2.get_brq_file import GetBRQFileHandler
from functions.a1_2.get_brq_file import GetBRQFileHandler, lambda_handler
from tests.mock_boto import mock_client_generator, mock_lambda_simple_return
from functions.BRQParser import BRQParser


GOOD_EVENT = json.load(open(os.path.dirname(__file__) + "/event_bus_msg.json"))

MOCK_ENV = {
    "EBOOKINGS_S3_FILEIN_BUCKET": "in-bucket",
    "EBOOKINGS_S3_TEMP_BUCKET": "temp-bucket",
    "SEIL_CONFIG_BUCKET_NAME": "seil-config-bucket",
    "SALESFORCE_ADAPTOR": "salesforce-mock-adaptor",
    "SEIL_AWS_REGION": "ap-southeast-2",
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

SALESFORCE_ADAPTOR_MOCK_RETURN_GET_BRQ_FILE_LIST_BAD = """
{
    "done": true,
    "records": [],
    "totalSize": 0
}"""

SALESFORCE_ADAPTOR_MOCK_RETURN_GET_BRQ_FILE_LIST_BAD_2 = """
{
    "done": true,
    "totalSize": 0
}"""

SEIL_AWS_REGION = "us-west-2"
PYTHON_VERSION = "python3.9"


# def mock_resource(type, region_name=""):
#     if type == "s3":
#         return MockS3Client(region_name=region_name)
#     elif type == "lambda":
#         return MockLambdaClient(region_name=region_name)


@mock.patch.dict("os.environ", MOCK_ENV, clear=True)
class TestGetBRQFileHandler:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        # cls.mock.stop()
        pass

    def test_get_latest_brq_file_info(self):

        with mock.patch(
            "boto3.client",
            mock_client_generator(
                {
                    "lambda": mock_lambda_simple_return(
                        json.loads(SALESFORCE_ADAPTOR_MOCK_RETURN_GET_BRQ_FILE_LIST)
                    )
                }
            ),
        ):
            handler = GetBRQFileHandler(
                json.loads(json.dumps(GOOD_EVENT))
            )  # create a copy of GOOD_EVENT
            handler._GetBRQFileHandler__get_latest_brq_file_info(
                "123"
            )  # private method calling method

        with mock.patch(
            "boto3.client",
            mock_client_generator(
                {
                    "lambda": mock_lambda_simple_return(
                        json.loads(SALESFORCE_ADAPTOR_MOCK_RETURN_GET_BRQ_FILE_LIST_BAD)
                    )
                }
            ),
        ):
            try:
                handler = GetBRQFileHandler(
                    json.loads(json.dumps(GOOD_EVENT))
                )  # create a copy of GOOD_EVENT
                handler._GetBRQFileHandler__get_latest_brq_file_info(
                    "123"
                )  # private method calling method
                raise NotImplementedError()  # it should not come to here as error raised
            except Exception as ex:
                assert (
                    str(ex)
                    == "No E-Booking BRQ file has been found for opportunity 123"
                )

        with mock.patch(
            "boto3.client",
            mock_client_generator(
                {
                    "lambda": mock_lambda_simple_return(
                        json.loads(
                            SALESFORCE_ADAPTOR_MOCK_RETURN_GET_BRQ_FILE_LIST_BAD_2
                        )
                    )
                }
            ),
        ):
            try:
                handler = GetBRQFileHandler(
                    json.loads(json.dumps(GOOD_EVENT))
                )  # create a copy of GOOD_EVENT
                handler._GetBRQFileHandler__get_latest_brq_file_info(
                    "123"
                )  # private method calling method
                raise NotImplementedError()  # it should not come to here as error raised
            except Exception as ex:
                assert (
                    str(ex)
                    == "No E-Booking BRQ file has been found for opportunity 123"
                )

    def test_read_brq_file_content(self):
        # class MockS3Client():
        #     def __init__(self, region_name):
        #         self.region_name = region_name
        #     def get_object(self, Bucket="", Key=""):
        #         mock_s3_object = {
        #             "Body": io.BytesIO("ABC".encode('utf-8'))
        #         }

        #         return mock_s3_object
        #     def put_object(self, *args):
        #         print("Inside mock put_object")
        #     def put(self, *args):
        #         print("Inside mock put")

        with mock.patch(
            "boto3.client",
            mock_client_generator({"lambda": mock_lambda_simple_return("ABC")}),
        ):
            handler = GetBRQFileHandler(json.loads(json.dumps(GOOD_EVENT)))
            file_content = handler._GetBRQFileHandler__read_brq_file_content(
                {
                    "Bucket": "test-bucket",
                    "Region": SEIL_AWS_REGION,
                    "Key": "test-key",
                    "record_id": "test-key",
                }
            )
            assert file_content == "ABC"

    def test_parse_brq_file(self):

        class MockBRQParser:
            def __init__(self, brq_file_content=""):
                print("In MockBRQParser")

            def parse(self):
                return {"mock-parse-result": True}

            def has_error(self):
                return False

        # this is mocking the "import" statement in the functions/a1_2/get_brq_file.py
        with mock.patch("functions.a1_2.get_brq_file.BRQParser", MockBRQParser):
            handler = GetBRQFileHandler(json.loads(json.dumps(GOOD_EVENT)))
            brq_object = handler._GetBRQFileHandler__parse_brq_file("ABC")
            assert brq_object["mock-parse-result"]

        class MockBRQParserBad:
            def __init__(self, brq_file_content=""):
                print("In MockBRQParser")

            def parse(self):
                return {"mock-parse-result": True}

            def has_error(self):
                return True

            def get_error(self):
                return "Mock Error"

        # this is mocking the "import" statement in the functions/a1_2/get_brq_file.py
        with mock.patch("functions.a1_2.get_brq_file.BRQParser", MockBRQParserBad):
            handler = GetBRQFileHandler(json.loads(json.dumps(GOOD_EVENT)))
            try:
                brq_object = handler._GetBRQFileHandler__parse_brq_file("ABC")
                raise NotImplementedError(
                    "It should raise an error in the previous line"
                )
            except Exception as ex:
                assert str(ex) == "BRQ Parse error: Mock Error"

    def test_write_temp_bucket(self):
        self.written_content = None

        class MockS3Client:
            def __init__(sself, region_name):
                pass

            def put_object(sself, Bucket, Key, Body):
                self.written_content = Body

        with mock.patch("boto3.client", mock_client_generator({"s3": MockS3Client})):
            handler = GetBRQFileHandler(json.loads(json.dumps(GOOD_EVENT)))
            handler._GetBRQFileHandler__write_temp_bucket({})
            assert self.written_content == "{}".encode("utf-8")

    def test_full_test(self):
        self.written_content = None

        class MockS3Client:
            def __init__(sself, region_name):
                pass

            def put_object(sself, Bucket, Key, Body):
                self.written_content = Body

            def get_object(sself, Bucket, Key):
                if (
                    Bucket == "swm-code7-r2-functest-creative"
                    and Key == "Opportunities/test_get_brq_file_test.brq"
                ):
                    with open(
                        os.path.join(
                            os.path.dirname(__file__),
                            "../brq_test_files/test_get_brq_file_test.brq",
                        )
                    ) as f:
                        content = f.read()
                    mock_s3_object = {"Body": io.BytesIO(content.encode("utf-8"))}
                    return mock_s3_object
                else:
                    raise NotImplementedError("Should not reach this path")

        # INPUT_EVENT = {
        #     "opportunityID": "006Bm000009ypDmAAA",
        #     "RecordTypeDeveloperName": "EBooking",
        #     "BRQRequestID": "Request123",
        #     "correlationID": "CorrelationID123"
        # }

        SF_FILE_LIST_RETURN = """
{
    "done": true,
    "records": [
        {
            "Id": "a91Bm00000020yPIAQ",
            "NEILON__Amazon_File_Key__c": "Opportunities/BiKe123.cskes@seven.com.au_SEVNET-2024-Request-000000307-SPARK.brq",
            "NEILON__Bucket_Name__c": "swm-code7-r2-functest-creative",
            "NEILON__Bucket_Region__c": "ap-southeast-2",
            "NEILON__Extension__c": ".brq",
            "NEILON__Opportunity__c": "006Bm000009ypDmAAA",
            "Name": "BiKe123.cskes@seven.com.au_SEVNET-2024-Request-000000307-SPARK.brq"
        }
    ],
    "totalSize": 1
}"""

        class MockLambdaClient:
            def __init__(self, type, region_name="") -> None:
                pass

            def invoke(self, FunctionName, InvocationType, Payload):
                json_payload = json.loads(Payload)
                if (
                    "invocationType" in json_payload
                    and json_payload["invocationType"] == "S3LINK_READFILECONTENT"
                ):
                    with open(
                        os.path.join(
                            os.path.dirname(__file__),
                            "../brq_test_files/test_get_brq_file_test.brq",
                        ),
                        "r",
                        encoding="utf-8",
                    ) as f:
                        content = f.read()
                    content = '"' + content + '"'  # the file content has double quote
                    return {"Payload": io.BytesIO(content.encode("utf-8"))}
                elif "record_id" in json_payload:
                    with open(
                        os.path.join(
                            os.path.dirname(__file__),
                            "../brq_test_files/test_get_brq_file_test.brq",
                        )
                    ) as f:
                        content = f.read()
                    # return content
                    return {"Payload": io.BytesIO(content.encode("utf-8"))}
                else:
                    return {"Payload": io.BytesIO(SF_FILE_LIST_RETURN.encode("utf-8"))}

        with mock.patch(
            "boto3.client",
            mock_client_generator({"s3": MockS3Client, "lambda": MockLambdaClient}),
        ):
            response = lambda_handler(json.loads(json.dumps(GOOD_EVENT)), {})
            print(response)

            brq_object = json.loads(self.written_content.decode("utf-8"))
            assert len(brq_object["details"]) == 6
            assert len(brq_object["narrativeRecords"]) == 0
            assert brq_object["header"]["GenerationDate"] == "20231219"
            assert brq_object["header"]["GenerationTime"] == "1052"

            assert response["brqJsonBucket"] == MOCK_ENV["EBOOKINGS_S3_TEMP_BUCKET"]
            assert (
                response["brqJsonKey"]
                == GOOD_EVENT["id"]
                + "/BiKe123.cskes@seven.com.au_SEVNET-2024-Request-000000307-SPARK.brq"
                + ".json"
            )
            assert (
                response["brqFileName"]
                == "BiKe123.cskes@seven.com.au_SEVNET-2024-Request-000000307-SPARK.brq"
            )
