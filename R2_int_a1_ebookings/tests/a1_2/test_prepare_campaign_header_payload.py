from unittest import mock
from unittest.mock import Mock, patch, MagicMock, ANY
import boto3
import os
import json
import io
import csv
from datetime import datetime
from tests.mock_boto import mock_client_generator, mock_lambda_simple_return

import sys

sys.path.append(
    os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
)  # project root folder

from functions.BRQParser import BRQParser

from functions.a1_2.prepare_campaign_header_payload import (
    PrepareCampaignHeaderPayloadHandler,
)

MOCK_ENV = {
    "EBOOKINGS_S3_FILEIN_BUCKET": "in-bucket",
    "EBOOKINGS_S3_TEMP_BUCKET": "temp-bucket",
    "SEIL_CONFIG_BUCKET_NAME": "seil-config-bucket",
    "SALESFORCE_ADAPTOR": "salesforce-mock-adaptor",
    # "SEIL_SALES_AREA_MAPPING_PARAM_NAME": "r2dev/salesareamapping_csv_file_path",
    "SEIL_SALES_AREA_MAPPING_PATH": "sales_area_mapping.csv",
    "SEIL_AWS_REGION": "ap-southeast-2",
    "LMK_DAYPART_ID": "a1/daypartid/path",
}

GOOD_EVENT = json.load(open(os.path.dirname(__file__) + "/event_bus_msg.json"))
GOOD_EVENT["brqJsonBucket"] = MOCK_ENV["EBOOKINGS_S3_TEMP_BUCKET"]
GOOD_EVENT["brqJsonKey"] = "test.brq.json"


# {
#     "opportunityID": "OpportunityID123",
#     "RecordTypeDeveloperName": "EBooking",
#     "BRQRequestID": "Request123",
#     "correlationID": "CorrelationID123",
#     "brqJsonBucket": MOCK_ENV["EBOOKINGS_S3_TEMP_BUCKET"],
#     "brqJsonKey": "test.brq",
#     "campaign": {
#         "campaignCode": "ABC"
#     }
# }


class MockSalesAreaMap:
    def __init__(self, param_name="", sales_area_path="", sales_area_content=None):
        print("test_prepare_spot_payload.MockSalesAreaMap: init...")
        csv_file_path = os.path.join(
            os.path.dirname(__file__), "sales_area_mapping.csv"
        )
        with open(csv_file_path, encoding="utf-8-sig") as csv_file_handler:
            sales_area_dict_reader = csv.DictReader(csv_file_handler)
            self._fieldnames = sales_area_dict_reader.fieldnames
            self._data = list(sales_area_dict_reader)

    @property
    def data(self):
        return self._data


@mock.patch.dict("os.environ", MOCK_ENV, clear=True)
class TestPrepareCampaignHeaderPayload:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    @mock.patch(
        "functions.a1_2.prepare_campaign_header_payload.SalesAreaMap", MockSalesAreaMap
    )
    def test_calculate_sales_area_for_one_parent_sales_area(self):
        class SSMMockClient:
            def __init__(self, region_name=""):
                pass

            def get_parameter(self, Name, WithDecryption=True):
                if Name == MOCK_ENV["LMK_DAYPART_ID"]:
                    mock_object = mock.MagicMock()
                    mock_object.Value = "35"
                    return {"Parameter": mock_object}
                else:
                    raise NotImplementedError()

        with mock.patch(
            "boto3.client",
            mock_client_generator(
                {
                    "ssm": SSMMockClient,
                }
            ),
        ):

            handler = PrepareCampaignHeaderPayloadHandler(
                json.loads(json.dumps(GOOD_EVENT))
            )
            one_parent_sales_area_details = [
                {"StationId": "1001"},
                {"StationId": "1001"},
                {"StationId": "1002"},
                {"StationId": "1002"},
                {"StationId": "1002"},
                {"StationId": "1003"},
                {"StationId": "1003"},
                {"StationId": "1003"},
                {"StationId": "1003"},
                {"StationId": "1004"},
            ]
            handler.brq_object = {"details": one_parent_sales_area_details}

            result = handler._PrepareCampaignHeaderPayloadHandler__calculate_sales_area_for_one_parent_sales_area(
                "1000", one_parent_sales_area_details, 100
            )

            salesArea = [x for x in result if x["salesAreaNumber"] == 1001][0]
            assert salesArea == {
                "salesAreaNumber": 1001,
                "isExcluded": False,
                "percentageSplit": 20,
                "spotsPercentage": 20,
            }

            salesArea = [x for x in result if x["salesAreaNumber"] == 1002][0]
            assert salesArea == {
                "salesAreaNumber": 1002,
                "isExcluded": False,
                "percentageSplit": 30,
                "spotsPercentage": 30,
            }

            salesArea = [x for x in result if x["salesAreaNumber"] == 1003][0]
            assert salesArea == {
                "salesAreaNumber": 1003,
                "isExcluded": False,
                "percentageSplit": 40,
                "spotsPercentage": 40,
            }

            salesArea = [x for x in result if x["salesAreaNumber"] == 1004][0]
            assert salesArea == {
                "salesAreaNumber": 1004,
                "isExcluded": False,
                "percentageSplit": 10,
                "spotsPercentage": 10,
            }

    @mock.patch(
        "functions.a1_2.prepare_campaign_header_payload.SalesAreaMap", MockSalesAreaMap
    )
    def test_calculate_strike_weigth_for_one_parent_sales_area(self):
        class SSMMockClient:
            def __init__(self, region_name=""):
                pass

            def get_parameter(self, Name, WithDecryption=True):
                if Name == MOCK_ENV["LMK_DAYPART_ID"]:
                    mock_object = mock.MagicMock()
                    mock_object.Value = "35"
                    return {"Parameter": mock_object}
                else:
                    raise NotImplementedError()

        with mock.patch(
            "boto3.client",
            mock_client_generator(
                {
                    "ssm": SSMMockClient,
                }
            ),
        ):
            handler = PrepareCampaignHeaderPayloadHandler(
                json.loads(json.dumps(GOOD_EVENT))
            )
            one_parent_sales_area_details = [
                {"StationId": "1001", "WCDate": "20231001"},
                {"StationId": "1001", "WCDate": "20231001"},
                {"StationId": "1002", "WCDate": "20231001"},
                {"StationId": "1002", "WCDate": "20231008"},
                {"StationId": "1002", "WCDate": "20231008"},
                {"StationId": "1003", "WCDate": "20231008"},
                {"StationId": "1003", "WCDate": "20231015"},
                {"StationId": "1003", "WCDate": "20231015"},
                {"StationId": "1003", "WCDate": "20231015"},
                {"StationId": "1004", "WCDate": "20231015"},
            ]
            handler.brq_object = {"details": one_parent_sales_area_details}
            result = handler._PrepareCampaignHeaderPayloadHandler__calculate_strike_weigth_for_one_parent_sales_area(
                one_parent_sales_area_details
            )
            assert result == [
                {
                    "period": {"endDate": "2023-10-07", "startDate": "2023-10-01"},
                    "ratingsPercentage": 33,
                    "spotsPercentage": 33,
                },
                {
                    "period": {"endDate": "2023-10-14", "startDate": "2023-10-08"},
                    "ratingsPercentage": 33,
                    "spotsPercentage": 33,
                },
                {
                    "period": {"endDate": "2023-10-21", "startDate": "2023-10-15"},
                    "ratingsPercentage": 34,
                    "spotsPercentage": 34,
                },
            ]

    @mock.patch(
        "functions.a1_2.prepare_campaign_header_payload.SalesAreaMap", MockSalesAreaMap
    )
    def test_calculate_delivery_length_for_one_parent_sales_area(self):
        class SSMMockClient:
            def __init__(self, region_name=""):
                pass

            def get_parameter(self, Name, WithDecryption=True):
                if Name == MOCK_ENV["LMK_DAYPART_ID"]:
                    mock_object = mock.MagicMock()
                    mock_object.Value = "35"
                    return {"Parameter": mock_object}
                else:
                    raise NotImplementedError()

        with mock.patch(
            "boto3.client",
            mock_client_generator(
                {
                    "ssm": SSMMockClient,
                }
            ),
        ):
            handler = PrepareCampaignHeaderPayloadHandler(
                json.loads(json.dumps(GOOD_EVENT))
            )
            one_parent_sales_area_details = [
                {"StationId": "1001", "WCDate": "20231001", "RequestedSize": 15},
                {"StationId": "1001", "WCDate": "20231001", "RequestedSize": 15},
                {"StationId": "1002", "WCDate": "20231001", "RequestedSize": 15},
                {"StationId": "1002", "WCDate": "20231008", "RequestedSize": 30},
                {"StationId": "1002", "WCDate": "20231008", "RequestedSize": 30},
                {"StationId": "1003", "WCDate": "20231008", "RequestedSize": 30},
                {"StationId": "1003", "WCDate": "20231015", "RequestedSize": 30},
                {"StationId": "1003", "WCDate": "20231015", "RequestedSize": 15},
                {"StationId": "1003", "WCDate": "20231015", "RequestedSize": 15},
                {"StationId": "1004", "WCDate": "20231015", "RequestedSize": 15},
            ]
            handler.brq_object = {"details": one_parent_sales_area_details}
            result = handler._PrepareCampaignHeaderPayloadHandler__calculate_delivery_length_for_one_parent_sales_area(
                one_parent_sales_area_details
            )
            assert result == [
                {"spotLength": 15, "percentage": (6 / 10 * 100)},
                {"spotLength": 30, "percentage": (4 / 10 * 100)},
            ]

    @mock.patch(
        "functions.a1_2.prepare_campaign_header_payload.SalesAreaMap", MockSalesAreaMap
    )
    def test_calculate_day_parts_for_one_parent_sales_area(self):
        class SSMMockClient:
            def __init__(self, region_name=""):
                pass

            def get_parameter(self, Name, WithDecryption=True):
                if Name == MOCK_ENV["LMK_DAYPART_ID"]:
                    mock_object = mock.MagicMock()
                    mock_object.Value = "35"
                    return {"Parameter": mock_object}
                else:
                    raise NotImplementedError()

        with mock.patch(
            "boto3.client",
            mock_client_generator(
                {
                    "ssm": SSMMockClient,
                }
            ),
        ):

            handler = PrepareCampaignHeaderPayloadHandler(
                json.loads(json.dumps(GOOD_EVENT))
            )
            one_parent_sales_area_details = [
                {"StationId": "1001", "WCDate": "20231001", "RequestedSize": 15},
                {"StationId": "1001", "WCDate": "20231001", "RequestedSize": 15},
                {"StationId": "1002", "WCDate": "20231001", "RequestedSize": 15},
                {"StationId": "1002", "WCDate": "20231008", "RequestedSize": 30},
                {"StationId": "1002", "WCDate": "20231008", "RequestedSize": 30},
                {"StationId": "1003", "WCDate": "20231008", "RequestedSize": 30},
                {"StationId": "1003", "WCDate": "20231015", "RequestedSize": 30},
                {"StationId": "1003", "WCDate": "20231015", "RequestedSize": 15},
                {"StationId": "1003", "WCDate": "20231015", "RequestedSize": 15},
                {"StationId": "1004", "WCDate": "20231015", "RequestedSize": 15},
            ]
            handler.brq_object = {"details": one_parent_sales_area_details}
            result = handler._PrepareCampaignHeaderPayloadHandler__calculate_day_parts_for_one_parent_sales_area(
                one_parent_sales_area_details
            )
            assert result == [
                {
                    "percentage": 100,
                    "daypartNameID": ANY,
                    "timeslices": [
                        {
                            "startDay": "Monday",
                            "endDay": "Sunday",
                            "startTime": "00:00:00",
                            "endTime": "23:59:59",
                        }
                    ],
                }
            ]

    @mock.patch(
        "functions.a1_2.prepare_campaign_header_payload.SalesAreaMap", MockSalesAreaMap
    )
    def test_group_by_parent_sales_area(self):
        class SSMMockClient:
            def __init__(self, region_name=""):
                pass

            def get_parameter(self, Name, WithDecryption=True):
                if Name == MOCK_ENV["LMK_DAYPART_ID"]:
                    mock_object = mock.MagicMock()
                    mock_object.Value = "31"
                    return {"Parameter": mock_object}
                else:
                    raise NotImplementedError()

        with mock.patch(
            "boto3.client",
            mock_client_generator(
                {
                    "ssm": SSMMockClient,
                }
            ),
        ):
            handler = PrepareCampaignHeaderPayloadHandler(
                json.loads(json.dumps(GOOD_EVENT))
            )

            with open(
                os.path.join(
                    os.path.dirname(__file__), "../brq_test_files/sales_area_test.brq"
                )
            ) as f:
                brq_parser = BRQParser(f.read())
                handler.brq_object = brq_parser.parse()

            handler.sales_area_map = MockSalesAreaMap()

            handler._PrepareCampaignHeaderPayloadHandler__group_by_parent_sales_area()

            assert "1000" in handler.brq_grouped_by_parent_sales_area
            assert "2000" in handler.brq_grouped_by_parent_sales_area
            assert len(handler.brq_grouped_by_parent_sales_area["1000"]) == 3
            assert len(handler.brq_grouped_by_parent_sales_area["2000"]) == 3

    def test_save_to_s3(self):
        self.written_content = None

        class MockS3Client:
            def __init__(sself, region_name):
                pass

            def put_object(sself, Bucket, Key, Body):
                self.written_content = Body

            def get_object(sself, Bucket, Key):
                pass

        class SSMMockClient:
            def __init__(self, region_name=""):
                pass

            def get_parameter(self, Name, WithDecryption=True):
                if Name == MOCK_ENV["LMK_DAYPART_ID"]:
                    mock_object = mock.MagicMock()
                    mock_object.Value = "31"
                    return {"Parameter": mock_object}
                else:
                    raise NotImplementedError()

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                with mock.patch(
                    "boto3.client",
                    mock_client_generator(
                        {
                            "s3": MockS3Client,
                            "ssm": SSMMockClient,
                        }
                    ),
                ):
                    mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                    handler = PrepareCampaignHeaderPayloadHandler(
                        json.loads(json.dumps(GOOD_EVENT))
                    )
                    # mock_json = {
                    #     "1": {"key1": "value1"},
                    #     "2": {"key2": "value2"}
                    # }
                    # handler.lmk_campaign_dict = mock_json
                    handler.lmk_upload_campaign_payload = expected_result = {
                        "approvalID": 1,
                        "approvalSourceID": 0,
                        "safeMode": False,
                        "campaigns": [
                            {
                                "campaignCode": "ABC",
                                "salesAreaOnCampaigns": [
                                    {"key1": "value1"},
                                    {"key2": "value2"},
                                ],
                            }
                        ],
                    }

                    # test the function
                    file_path = (
                        handler._PrepareCampaignHeaderPayloadHandler__save_to_s3()
                    )

                    assert self.written_content == json.dumps(expected_result)
                    assert (
                        file_path
                        == "s3://temp-bucket/b05b0bca-2f88-40eb-ee6e-6344671ba03e/campaign_header_payload.json"
                    )

    @mock.patch(
        "functions.a1_2.prepare_campaign_header_payload.SalesAreaMap", MockSalesAreaMap
    )
    def test_calculate_campaign(self):
        class SSMMockClient:
            def __init__(self, region_name=""):
                pass

            def get_parameter(self, Name, WithDecryption=True):
                if Name == MOCK_ENV["LMK_DAYPART_ID"]:
                    mock_object = mock.MagicMock()
                    mock_object.Value = "31"
                    return {"Parameter": mock_object}
                else:
                    raise NotImplementedError()

        with mock.patch(
            "boto3.client",
            mock_client_generator(
                {
                    "ssm": SSMMockClient,
                }
            ),
        ):
            handler = PrepareCampaignHeaderPayloadHandler(
                json.loads(json.dumps(GOOD_EVENT))
            )

            with open(
                os.path.join(
                    os.path.dirname(__file__), "../brq_test_files/sales_area_test.brq"
                )
            ) as f:
                brq_parser = BRQParser(f.read())
                handler.brq_object = brq_parser.parse()

            handler.sales_area_map = MockSalesAreaMap()
            handler._PrepareCampaignHeaderPayloadHandler__group_by_parent_sales_area()
            handler._PrepareCampaignHeaderPayloadHandler__calculate_campaign()

            ROW_COUNT_IN_BRQ = 6

            expected_result = {
                1000: {
                    "salesAreaNumber": 1000,
                    "percentageSplit": 33.33,
                    "deliveryCurrencyPricing": {
                        "deliveryCurrencyType": "NumberOfSpots",
                        "deliveryCurrencyPriceValue": 3,
                    },
                    "salesAreaDetails": [
                        {
                            "salesAreaNumber": 1001,
                            "isExcluded": False,
                            "percentageSplit": 11.11,
                            "spotsPercentage": 11.11,
                        },
                        {
                            "salesAreaNumber": 1002,
                            "isExcluded": False,
                            "percentageSplit": 11.11,
                            "spotsPercentage": 11.11,
                        },
                        {
                            "salesAreaNumber": 1003,
                            "isExcluded": False,
                            "percentageSplit": 11.11,
                            "spotsPercentage": 11.11,
                        },
                        {
                            "salesAreaNumber": 1004,
                            "isExcluded": False,
                            "percentageSplit": 0,
                        },
                        {
                            "salesAreaNumber": 1005,
                            "isExcluded": False,
                            "percentageSplit": 0,
                        },
                    ],
                    "deliveryLengths": [
                        {"spotLength": 15, "percentage": 66.66},
                        {"spotLength": 30, "percentage": 33.34},
                    ],
                    "dayparts": [
                        {
                            "percentage": 100,
                            "daypartNameID": ANY,
                            "timeslices": [
                                {
                                    "startDay": "Monday",
                                    "endDay": "Sunday",
                                    "startTime": "00:00:00",
                                    "endTime": "23:59:59",
                                }
                            ],
                        }
                    ],
                    "strikeWeights": [
                        {
                            "period": {
                                "startDate": "2024-01-07",
                                "endDate": "2024-01-13",
                            },
                            "ratingsPercentage": 50,
                            "spotsPercentage": 50,
                        },
                        {
                            "period": {
                                "startDate": "2024-01-14",
                                "endDate": "2024-01-20",
                            },
                            "ratingsPercentage": 50,
                            "spotsPercentage": 50,
                        },
                    ],
                },
                2000: {
                    "salesAreaNumber": 2000,
                    "percentageSplit": 33.33,
                    "deliveryCurrencyPricing": {
                        "deliveryCurrencyType": "NumberOfSpots",
                        "deliveryCurrencyPriceValue": 3,
                    },
                    "salesAreaDetails": [
                        {
                            "salesAreaNumber": 2001,
                            "isExcluded": False,
                            "percentageSplit": 11.11,
                            "spotsPercentage": 11.11,
                        },
                        {
                            "salesAreaNumber": 2002,
                            "isExcluded": False,
                            "percentageSplit": 11.11,
                            "spotsPercentage": 11.11,
                        },
                        {
                            "salesAreaNumber": 2003,
                            "isExcluded": False,
                            "percentageSplit": 11.11,
                            "spotsPercentage": 11.11,
                        },
                        {
                            "salesAreaNumber": 2004,
                            "isExcluded": False,
                            "percentageSplit": 0,
                        },
                        {
                            "salesAreaNumber": 2005,
                            "isExcluded": False,
                            "percentageSplit": 0,
                        },
                    ],
                    "deliveryLengths": [
                        {"spotLength": 15, "percentage": 66.66},
                        {"spotLength": 30, "percentage": 33.34},
                    ],
                    "dayparts": [
                        {
                            "percentage": 100,
                            "daypartNameID": ANY,
                            "timeslices": [
                                {
                                    "startDay": "Monday",
                                    "endDay": "Sunday",
                                    "startTime": "00:00:00",
                                    "endTime": "23:59:59",
                                }
                            ],
                        }
                    ],
                    "strikeWeights": [
                        {
                            "period": {
                                "startDate": "2024-01-07",
                                "endDate": "2024-01-13",
                            },
                            "ratingsPercentage": 50,
                            "spotsPercentage": 50,
                        },
                        {
                            "period": {
                                "startDate": "2024-01-14",
                                "endDate": "2024-01-20",
                            },
                            "ratingsPercentage": 50,
                            "spotsPercentage": 50,
                        },
                    ],
                },
                3000: {
                    "salesAreaNumber": 3000,
                    "percentageSplit": 33.34,
                    "deliveryCurrencyPricing": {
                        "deliveryCurrencyType": "NumberOfSpots",
                        "deliveryCurrencyPriceValue": 3,
                    },
                    "salesAreaDetails": [
                        {
                            "salesAreaNumber": 3001,
                            "isExcluded": False,
                            "percentageSplit": 11.11,
                            "spotsPercentage": 11.11,
                        },
                        {
                            "salesAreaNumber": 3002,
                            "isExcluded": False,
                            "percentageSplit": 11.11,
                            "spotsPercentage": 11.11,
                        },
                        {
                            "salesAreaNumber": 3003,
                            "isExcluded": False,
                            "percentageSplit": 11.12,
                            "spotsPercentage": 11.12,
                        },
                        {
                            "salesAreaNumber": 3004,
                            "isExcluded": False,
                            "percentageSplit": 0,
                        },
                        {
                            "salesAreaNumber": 3005,
                            "isExcluded": False,
                            "percentageSplit": 0,
                        },
                    ],
                    "deliveryLengths": [
                        {"spotLength": 15, "percentage": 66.66},
                        {"spotLength": 30, "percentage": 33.34},
                    ],
                    "dayparts": [
                        {
                            "percentage": 100,
                            "daypartNameID": ANY,
                            "timeslices": [
                                {
                                    "startDay": "Monday",
                                    "endDay": "Sunday",
                                    "startTime": "00:00:00",
                                    "endTime": "23:59:59",
                                }
                            ],
                        }
                    ],
                    "strikeWeights": [
                        {
                            "period": {
                                "startDate": "2024-01-07",
                                "endDate": "2024-01-13",
                            },
                            "ratingsPercentage": 50,
                            "spotsPercentage": 50,
                        },
                        {
                            "period": {
                                "startDate": "2024-01-14",
                                "endDate": "2024-01-20",
                            },
                            "ratingsPercentage": 50,
                            "spotsPercentage": 50,
                        },
                    ],
                },
            }
            assert handler.lmk_campaign_dict == expected_result

    @mock.patch(
        "functions.a1_2.prepare_campaign_header_payload.SalesAreaMap", MockSalesAreaMap
    )
    def test_handle(self):

        class MockS3Client:
            def __init__(sself, region_name):
                pass

            def put_object(sself, Bucket, Key, Body):
                self.written_content = Body

            def get_object(sself, Bucket, Key):
                if Key == "test_no_default.brq.json":
                    file_path = os.path.join(
                        os.path.dirname(__file__),
                        "../brq_test_files/sales_area_test_no_default.brq",
                    )
                elif Bucket == MOCK_ENV["EBOOKINGS_S3_TEMP_BUCKET"]:
                    file_path = os.path.join(
                        os.path.dirname(__file__),
                        "../brq_test_files/sales_area_test.brq",
                    )

                parser = BRQParser(open(file_path).read())
                parser.parse()
                return {"Body": io.BytesIO(json.dumps(parser.json).encode("utf-8"))}

        class SSMMockClient:
            def __init__(self, region_name=""):
                pass

            def get_parameter(self, Name, WithDecryption=True):
                if Name == MOCK_ENV["LMK_DAYPART_ID"]:
                    mock_object = mock.MagicMock()
                    # mock_object.Value = "31"
                    return {"Parameter": {"Value": "31"}}
                else:
                    raise NotImplementedError()

        expected = json.loads(
            open(
                os.path.dirname(__file__) + "/event_bus_msg_expected_payload.json"
            ).read()
        )

        with mock.patch(
            "functions.a1_2.prepare_campaign_header_payload.SalesAreaMap",
            MockSalesAreaMap,
        ):
            with mock.patch(
                "boto3.client",
                mock_client_generator({"ssm": SSMMockClient, "s3": MockS3Client}),
            ):

                handler = PrepareCampaignHeaderPayloadHandler(
                    json.loads(json.dumps(GOOD_EVENT))
                )
                response = handler.handle()
                assert json.loads(self.written_content) == expected

                # This is the test case to remove the default SalesArea
                expected = json.loads(
                    open(
                        os.path.dirname(__file__)
                        + "/event_bus_msg_no_default_expected_payload.json"
                    ).read()
                )

                NO_DEFAULT_EVENT = json.load(
                    open(os.path.dirname(__file__) + "/event_bus_msg.json")
                )
                NO_DEFAULT_EVENT["brqJsonBucket"] = MOCK_ENV["EBOOKINGS_S3_TEMP_BUCKET"]
                NO_DEFAULT_EVENT["brqJsonKey"] = "test_no_default.brq.json"
                handler = PrepareCampaignHeaderPayloadHandler(
                    json.loads(json.dumps(NO_DEFAULT_EVENT))
                )
                response = handler.handle()
                assert json.loads(self.written_content) == expected

        pass
