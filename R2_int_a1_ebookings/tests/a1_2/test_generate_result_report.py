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


# from functions.a1_2.get_brq_file import GetBRQFileHandler
from functions.a1_2.generate_result_report import ResultReportGenerator
from tests.mock_boto import mock_client_generator, mock_lambda_simple_return
from functions.BRQParser import BRQParser

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
        },
        "campaign_code": 278,
    },
    "spotPrebookingResponsePath": "s3://temp-bucket/71f18a84-7c6a-cd0e-6f53-8ddc2c10983d/spots_repsonse.json",
    "brqJsonBucket": "temp-bucket",
    "brqJsonKey": "brq.json",
    "brqRequestID": "000000307-SPARK",
}

MOCK_ENV = {
    "EBOOKINGS_S3_FILEIN_BUCKET": "in-bucket",
    "EBOOKINGS_S3_TEMP_BUCKET": "temp-bucket",
    "SEIL_CONFIG_BUCKET_NAME": "seil-config-bucket",
    "SALESFORCE_ADAPTOR": "salesforce-mock-adaptor",
    "SEIL_AWS_REGION": "ap-southeast-2",
}
print("__file__")
print(__file__)
print(os.path.dirname(__file__))
print(os.path.dirname(__file__) + "/report_test_json/brq.json")

CONTENT_BRQ_JSON = open(os.path.dirname(__file__) + "/report_test_json/brq.json").read()
CONTENT_CAMPAIGN_HEADER_PAYLOAD_JSON = open(
    os.path.dirname(__file__) + "/report_test_json/campaign_header_payload.json"
).read()
CONTENT_CAMPAIGN_HEADER_RESPONSE_JSON = open(
    os.path.dirname(__file__) + "/report_test_json/campaign_header_response.json"
).read()
CONTENT_SPOTS_PAYLOAD_JSON = open(
    os.path.dirname(__file__) + "/report_test_json/spots_payload.json"
).read()
CONTENT_SPOTS_RESPONSE_JSON = open(
    os.path.dirname(__file__) + "/report_test_json/spots_response.json"
).read()


class MockS3Client:
    def __init__(self, region_name):
        pass

    def put_object(self, Bucket, Key, Body):
        self.written_content = Body

    def get_object(self, Bucket, Key):
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


@mock.patch.dict("os.environ", MOCK_ENV, clear=True)
class TestResultReportGenerator:
    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        # cls.mock.stop()
        pass

    def test_list_to_status_detail(self):
        handler = ResultReportGenerator(GOOD_EVENT)

        partial_response_json = [
            {
                "campaignNumber": 337,
                "lineNumber": 1,
                "messages": [
                    {
                        "type": "urn:lmks:bll:23784",
                        "title": "Title1-1",
                        "status": 400,
                        "detail": "Msg1-1",
                    },
                    {
                        "type": "urn:lmks:bll:23792",
                        "title": "Title1-2",
                        "status": 422,
                        "detail": "Msg1-2",
                    },
                ],
            },
            {
                "campaignNumber": 337,
                "lineNumber": 2,
                "messages": [
                    {
                        "type": "urn:lmks:bll:23784",
                        "title": "Title2-1",
                        "status": 422,
                        "detail": "Msg2-1",
                    },
                    {
                        "type": "urn:lmks:bll:23792",
                        "title": "Title2-2",
                        "status": 400,
                        "detail": "Msg2-2",
                    },
                ],
            },
            {
                "campaignNumber": 337,
                "lineNumber": 3,
                "messages": [
                    {
                        "type": "urn:lmks:bll:23784",
                        "title": "Success3-1",
                        "status": 200,
                        "detail": "Success3-1",
                    },
                    {
                        "type": "urn:lmks:bll:23792",
                        "title": "Success3-2",
                        "status": 202,
                        "detail": "Success3-2",
                    },
                ],
            },
        ]

        expected = {
            0: {"title": "Title1-1;Title1-2", "status": 422, "msg": "Msg1-1;Msg1-2"},
            1: {"title": "Title2-1;Title2-2", "status": 422, "msg": "Msg2-1;Msg2-2"},
        }

        handler.spot_payload_json = json.loads(CONTENT_SPOTS_PAYLOAD_JSON)
        actual = handler._ResultReportGenerator__list_to_status_detail(
            partial_response_json
        )
        assert actual == (expected, "Partial")

        all_success_response_json = [
            {
                "campaignNumber": 337,
                "lineNumber": 1,
                "messages": [
                    {
                        "type": "urn:lmks:bll:23784",
                        "title": "Title1-1",
                        "status": 202,
                        "detail": "Msg1-1",
                    },
                    {
                        "type": "urn:lmks:bll:23792",
                        "title": "Title1-2",
                        "status": 202,
                        "detail": "Msg1-2",
                    },
                ],
            },
            {
                "campaignNumber": 337,
                "lineNumber": 2,
                "messages": [
                    {
                        "type": "urn:lmks:bll:23784",
                        "title": "Title2-1",
                        "status": 202,
                        "detail": "Msg2-1",
                    },
                    {
                        "type": "urn:lmks:bll:23792",
                        "title": "Title2-2",
                        "status": 202,
                        "detail": "Msg2-2",
                    },
                ],
            },
            {
                "campaignNumber": 337,
                "lineNumber": 3,
                "messages": [
                    {
                        "type": "urn:lmks:bll:23784",
                        "title": "Success3-1",
                        "status": 200,
                        "detail": "Success3-1",
                    },
                    {
                        "type": "urn:lmks:bll:23792",
                        "title": "Success3-2",
                        "status": 202,
                        "detail": "Success3-2",
                    },
                ],
            },
        ]

        expected = {}

        handler.spot_payload_json = json.loads(CONTENT_SPOTS_PAYLOAD_JSON)
        actual = handler._ResultReportGenerator__list_to_status_detail(
            all_success_response_json
        )
        assert actual == (expected, "Yes")

        all_failed_response_json = [
            {
                "campaignNumber": 337,
                "lineNumber": 1,
                "messages": [
                    {
                        "type": "urn:lmks:bll:23784",
                        "title": "Title1-1",
                        "status": 400,
                        "detail": "Msg1-1",
                    },
                    {
                        "type": "urn:lmks:bll:23792",
                        "title": "Title1-2",
                        "status": 422,
                        "detail": "Msg1-2",
                    },
                ],
            },
            {
                "campaignNumber": 337,
                "lineNumber": 2,
                "messages": [
                    {
                        "type": "urn:lmks:bll:23784",
                        "title": "Title2-1",
                        "status": 422,
                        "detail": "Msg2-1",
                    },
                    {
                        "type": "urn:lmks:bll:23792",
                        "title": "Title2-2",
                        "status": 400,
                        "detail": "Msg2-2",
                    },
                ],
            },
            {
                "campaignNumber": 337,
                "lineNumber": 3,
                "messages": [
                    {
                        "type": "urn:lmks:bll:23784",
                        "title": "Title3-1",
                        "status": 400,
                        "detail": "Msg3-1",
                    },
                    {
                        "type": "urn:lmks:bll:23792",
                        "title": "Title3-2",
                        "status": 400,
                        "detail": "Msg3-2",
                    },
                ],
            },
        ]

        expected = {
            0: {"title": "Title1-1;Title1-2", "status": 422, "msg": "Msg1-1;Msg1-2"},
            1: {"title": "Title2-1;Title2-2", "status": 422, "msg": "Msg2-1;Msg2-2"},
            2: {"title": "Title3-1;Title3-2", "status": 400, "msg": "Msg3-1;Msg3-2"},
        }

        handler.spot_payload_json = json.loads(CONTENT_SPOTS_PAYLOAD_JSON)
        actual = handler._ResultReportGenerator__list_to_status_detail(
            all_failed_response_json
        )
        assert actual == (expected, "No")

    def test_generate(self):

        self.written_content = ""

        class MockS3Client:
            def __init__(sself, *args) -> None:
                pass

            def get_object(sself, Bucket, Key, *args):
                if Bucket == MOCK_ENV["EBOOKINGS_S3_TEMP_BUCKET"]:
                    file_name = os.path.basename(Key)
                    content = open(
                        os.path.dirname(__file__) + "/report_test_json/" + file_name
                    ).read()
                    return {"Body": io.BytesIO(content.encode("utf-8"))}

            def put_object(sself, Body, Bucket, Key, *args):
                self.written_content = Body

        expected_csv = """
"Agency Name","Client Name","Client Product Name","Station Name","W/C Date","Days","Time","Program","Duration","Rate","Demographic Code One","Demographic One Tarp","Demographic One Thousand","Proposed Agency Spot ID","Booking Modifiers","title","status","detail"
"R2SIT_SPARK FOUNDRY","R2SIT_Hello Fresh","Every Plate AU","7 Wagga","20240204","NNNNNYN","06000900","Sunrise",15,69.0,"P40+",12.0,0.0,"0000028244-000000001","ABCDEF","Validation/Save failed;Validation/Save failed;Validation/Save failed",422,"Length must match the Campaign length.;Break Sales Area Code does not exists or not valid for Campaign.;Spot Sales Area Code does not exists or not valid for Campaign."
"R2SIT_SPARK FOUNDRY","R2SIT_Hello Fresh","Every Plate AU","7 Wagga","20240204","NNNNNYN","06000900","Sunrise",15,69.0,"P40+",12.0,0.0,"0000028244-000000002","","Validation/Save failed;Validation/Save failed;Validation/Save failed",422,"Length must match the Campaign length.;Break Sales Area Code does not exists or not valid for Campaign.;Spot Sales Area Code does not exists or not valid for Campaign."
"R2SIT_SPARK FOUNDRY","R2SIT_Hello Fresh","Every Plate AU","7 Wagga","20240204","NNNYNNN","06000900","Sunrise",15,69.0,"P40+",12.0,0.0,"0000028244-000000005","","Validation/Save failed;Validation/Save failed;Validation/Save failed",422,"Length must match the Campaign length.;Break Sales Area Code does not exists or not valid for Campaign.;Spot Sales Area Code does not exists or not valid for Campaign."
"R2SIT_SPARK FOUNDRY","R2SIT_Hello Fresh","Every Plate AU","7 Wagga","20240204","NNNNYNN","06000900","Sunrise",15,69.0,"P40+",12.0,0.0,"0000028244-000000006","","Validation/Save failed;Validation/Save failed;Validation/Save failed",422,"Length must match the Campaign length.;Break Sales Area Code does not exists or not valid for Campaign.;Spot Sales Area Code does not exists or not valid for Campaign."
""".strip()

        with mock.patch("boto3.client", mock_client_generator({"s3": MockS3Client})):
            with mock.patch(
                "functions.a1_2.generate_result_report.datetime", wrap=datetime
            ) as mock_datetime:
                mock_datetime.now.return_value = datetime(2024, 1, 24, 15, 14, 38)

                generator = ResultReportGenerator(GOOD_EVENT)
                output = generator.generate()

                assert generator.overall_status == "Partial"
                assert (
                    generator.report_file_name
                    == "SpotsNotLoadedinLMK-000000307-SPARK-OpportunityID123-278-2024-01-24_151438.csv"
                )
                assert (
                    generator.report_file_uri
                    == "s3://temp-bucket/71f18a84-7c6a-cd0e-6f53-8ddc2c10983d/SpotsNotLoadedinLMK-000000307-SPARK-OpportunityID123-278-2024-01-24_151438.csv"
                )
                assert (
                    self.written_content.decode("utf-8").splitlines()
                    == expected_csv.splitlines()
                )  # assert line by line to avoid "\r\n" and "\n" different

                assert output == {
                    "spotPrebookingReport": "s3://temp-bucket/71f18a84-7c6a-cd0e-6f53-8ddc2c10983d/SpotsNotLoadedinLMK-000000307-SPARK-OpportunityID123-278-2024-01-24_151438.csv",
                    "spotPrebookingResultStatus": "Partial",
                    "spotPrebookingResultFileNameToSF": "SpotsNotLoadedinLMK-000000307-SPARK-OpportunityID123-278-2024-01-24_151438.csv",
                }
