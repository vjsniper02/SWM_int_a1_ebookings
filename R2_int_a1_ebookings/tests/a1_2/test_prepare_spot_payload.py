from unittest import mock
import os
import csv
from datetime import datetime
import io
import json

import sys

sys.path.append(
    os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
)  # project root folder

from functions.a1_2.prepare_spot_payload import PrepareSpotsPayloadHandler
from tests.mock_boto import mock_client_generator, mock_lambda_simple_return

MOCK_ENV = {
    "EBOOKINGS_S3_FILEIN_BUCKET": "in-bucket",
    "EBOOKINGS_S3_TEMP_BUCKET": "temp-bucket",
    "SEIL_CONFIG_BUCKET_NAME": "seil-config-bucket",
    "SALESFORCE_ADAPTOR": "salesforce-mock-adaptor",
    # "SEIL_SALES_AREA_MAPPING_PARAM_NAME": "r2dev/salesareamapping_csv_file_path",
    "SEIL_SALES_AREA_MAPPING_PATH": "s3://config-bucket/sales-area-mapping.csv",
    "SEIL_AWS_REGION": "ap-southeast-2",
}

GOOD_EVENT = json.load(open(os.path.dirname(__file__) + "/event_bus_msg.json"))
GOOD_EVENT["brqJsonBucket"] = MOCK_ENV["EBOOKINGS_S3_TEMP_BUCKET"]
GOOD_EVENT["brqJsonKey"] = "test.json"


class MockSalesAreaMap:
    def __init__(
        self,
        param_name: str = None,
        sales_area_path: str = None,
        sales_area_csv: str = None,
    ):
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

    def test_prepare_spot_payload(self):
        mock_brq_object = {
            "header": {"campaignCode": 1234567890},
            "details": [
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "111",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "112",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                },
            ],
        }

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                handler = PrepareSpotsPayloadHandler(GOOD_EVENT)
                handler.brq_object = mock_brq_object

                # test the function
                handler._PrepareSpotsPayloadHandler__prepare_spot_payload()

                assert handler.spot_full_payload == {
                    "dateTimeStamp": "2023-12-15T13:01:59",
                    "spotPreBookingDetails": [
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 111,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 112,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                    ],
                }

    def test_save_to_s3(self):
        self.written_content = None

        class MockS3Client:
            def __init__(sself, region_name):
                pass

            def put_object(sself, Bucket, Key, Body):
                self.written_content = Body

            def get_object(sself, Bucket, Key):
                pass

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                with mock.patch(
                    "boto3.client", mock_client_generator({"s3": MockS3Client})
                ) as mock_s3_client:
                    mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                    handler = PrepareSpotsPayloadHandler(GOOD_EVENT)
                    mock_json = {"key1": 123}
                    handler._spot_full_payload = mock_json

                    # test the function
                    file_path = handler._PrepareSpotsPayloadHandler__save_to_s3()

                    assert self.written_content == json.dumps(mock_json)
                    assert (
                        file_path
                        == "s3://temp-bucket/b05b0bca-2f88-40eb-ee6e-6344671ba03e/spots_payload.json"
                    )

    def test_multiparts_TP_TA(self):
        mock_brq_object = {
            "header": {"campaignCode": 1234567890},
            "details": [
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "111",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 30,
                    "BookingModifiers": ["TP"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "112",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.2,
                    "RequestedGrossRate": 1.12,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["TA"],
                },
            ],
        }

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                handler = PrepareSpotsPayloadHandler(GOOD_EVENT)
                handler.brq_object = mock_brq_object

                # test the function
                handler._PrepareSpotsPayloadHandler__prepare_spot_payload()

                assert handler.spot_full_payload == {
                    "dateTimeStamp": "2023-12-15T13:01:59",
                    "spotPreBookingDetails": [
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 111,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 30,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                            "multiparts": [{"length": 15, "extraFloatData2": 1.12}],
                        }
                    ],
                }

    def test_multiparts_TP_MD(self):
        mock_brq_object = {
            "header": {"campaignCode": 1234567890},
            "details": [
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "111",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 30,
                    "BookingModifiers": ["TP"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "112",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.2,
                    "RequestedGrossRate": 1.12,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["MD"],
                },
            ],
        }

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                handler = PrepareSpotsPayloadHandler(GOOD_EVENT)
                handler.brq_object = mock_brq_object

                # test the function
                handler._PrepareSpotsPayloadHandler__prepare_spot_payload()

                assert handler.spot_full_payload == {
                    "dateTimeStamp": "2023-12-15T13:01:59",
                    "spotPreBookingDetails": [
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 111,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 30,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                            "multiparts": [{"length": 15, "extraFloatData2": 1.12}],
                        }
                    ],
                }

    def test_multiparts_TP_MD_TA_Empty(self):
        mock_brq_object = {
            "header": {"campaignCode": 1234567890},
            "details": [
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "111",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 30,
                    "BookingModifiers": ["TP"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "112",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.2,
                    "RequestedGrossRate": 1.12,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["MD"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "113",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.3,
                    "RequestedGrossRate": 1.13,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["TA"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "114",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.4,
                    "RequestedGrossRate": 1.14,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                },
            ],
        }

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                handler = PrepareSpotsPayloadHandler(GOOD_EVENT)
                handler.brq_object = mock_brq_object

                # test the function
                handler._PrepareSpotsPayloadHandler__prepare_spot_payload()

                assert handler.spot_full_payload == {
                    "dateTimeStamp": "2023-12-15T13:01:59",
                    "spotPreBookingDetails": [
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 111,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 30,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                            "multiparts": [
                                {"length": 15, "extraFloatData2": 1.12},
                                {"length": 15, "extraFloatData2": 1.13},
                            ],
                        },
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 114,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 1.14,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                    ],
                }

    def test_multiparts_TP_MD_Empty_TP_TA(self):
        mock_brq_object = {
            "header": {"campaignCode": 1234567890},
            "details": [
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "111",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.1,
                    "RequestedGrossRate": 1.11,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 30,
                    "BookingModifiers": ["TP"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "112",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.2,
                    "RequestedGrossRate": 1.12,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["MD"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "113",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.3,
                    "RequestedGrossRate": 1.13,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    # "BookingModifiers": [ ] # Empty
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "114",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.4,
                    "RequestedGrossRate": 1.14,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 45,
                    "BookingModifiers": ["TP"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "115",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.5,
                    "RequestedGrossRate": 1.15,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["TA"],
                },
            ],
        }

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                handler = PrepareSpotsPayloadHandler(GOOD_EVENT)
                handler.brq_object = mock_brq_object

                # test the function
                handler._PrepareSpotsPayloadHandler__prepare_spot_payload()

                assert handler.spot_full_payload == {
                    "dateTimeStamp": "2023-12-15T13:01:59",
                    "spotPreBookingDetails": [
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 111,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 30,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 1.11,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                            "multiparts": [{"length": 15, "extraFloatData2": 1.12}],
                        },
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 113,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 1.13,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 114,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 45,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 1.14,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                            "multiparts": [{"length": 15, "extraFloatData2": 1.15}],
                        },
                    ],
                }

    def test_multiparts_TP_MD_TA_Empty_TA(self):
        mock_brq_object = {
            "header": {"campaignCode": 1234567890},
            "details": [
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "111",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.1,
                    "RequestedGrossRate": 1.11,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 45,
                    "BookingModifiers": ["TP"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "112",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.2,
                    "RequestedGrossRate": 1.12,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 30,
                    "BookingModifiers": ["MD"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "113",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.3,
                    "RequestedGrossRate": 1.13,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["TA"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "114",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.4,
                    "RequestedGrossRate": 1.14,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    # "BookingModifiers": [ ] # Empty
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "115",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 11.5,
                    "RequestedGrossRate": 1.15,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["TA"],
                },
            ],
        }

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                handler = PrepareSpotsPayloadHandler(GOOD_EVENT)
                handler.brq_object = mock_brq_object

                # test the function
                handler._PrepareSpotsPayloadHandler__prepare_spot_payload()

                assert handler.spot_full_payload == {
                    "dateTimeStamp": "2023-12-15T13:01:59",
                    "spotPreBookingDetails": [
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 111,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 45,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 1.11,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                            "multiparts": [
                                {"length": 30, "extraFloatData2": 1.12},
                                {"length": 15, "extraFloatData2": 1.13},
                            ],
                        },
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 114,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 1.14,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 115,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 1.15,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                    ],
                }

    def test_multiparts_TP_No_MD_TA(self):
        mock_brq_object = {
            "header": {"campaignCode": 1234567890},
            "details": [
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "111",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["TP"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "112",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                },
            ],
        }

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                handler = PrepareSpotsPayloadHandler(GOOD_EVENT)
                handler.brq_object = mock_brq_object

                # test the function
                handler._PrepareSpotsPayloadHandler__prepare_spot_payload()

                assert handler.spot_full_payload == {
                    "dateTimeStamp": "2023-12-15T13:01:59",
                    "spotPreBookingDetails": [
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 111,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 112,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                    ],
                }

    def test_multiparts_MA_No_TP(self):
        mock_brq_object = {
            "header": {"campaignCode": 1234567890},
            "details": [
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "111",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["MD"],
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "112",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["TA"],
                },
            ],
        }

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                handler = PrepareSpotsPayloadHandler(GOOD_EVENT)
                handler.brq_object = mock_brq_object

                # test the function
                handler._PrepareSpotsPayloadHandler__prepare_spot_payload()

                assert handler.spot_full_payload == {
                    "dateTimeStamp": "2023-12-15T13:01:59",
                    "spotPreBookingDetails": [
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 111,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 112,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                    ],
                }

    def test_multiparts_Last_TP(self):
        mock_brq_object = {
            "header": {"campaignCode": 1234567890},
            "details": [
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "111",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                },
                {
                    "StationId": "SAS",
                    "UniqueNetworkProposedSpotId": "112",
                    "RequestedTime": "06000900",
                    "DemographicOneTarp": 10.00,
                    "RequestedNetRate": 12.34,
                    "RequestedGrossRate": 23.45,
                    "RequestedProgram": "ProgramName",
                    "RequestedDay": "NNYNNNN",
                    "WCDate": "20231001",
                    "RequestedSize": 15,
                    "BookingModifiers": ["TP"],
                },
            ],
        }

        with mock.patch(
            "functions.a1_2.prepare_spot_payload.SalesAreaMap", MockSalesAreaMap
        ):
            with mock.patch(
                "functions.a1_2.prepare_spot_payload.datetime", wraps=datetime
            ) as mock_datetime:  # "wraps" will retain all the datetime functions
                mock_datetime.now.return_value = datetime(2023, 12, 15, 13, 1, 59)

                handler = PrepareSpotsPayloadHandler(GOOD_EVENT)
                handler.brq_object = mock_brq_object

                # test the function
                handler._PrepareSpotsPayloadHandler__prepare_spot_payload()

                assert handler.spot_full_payload == {
                    "dateTimeStamp": "2023-12-15T13:01:59",
                    "spotPreBookingDetails": [
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 111,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                        {
                            "campaignNumber": 338,
                            "breakSalesAreaCode": "A1",
                            "lineNumber": 112,
                            "versionNumber": 1,
                            "spotSalesAreaCode": "A1",
                            "scheduledDate": "2023-10-03",
                            "slotStartTime": "06:00:00",
                            "slotEndTime": "09:00:00",
                            "length": 15,
                            "businessTypeCode": "PDS",
                            "bookingType": 2,
                            "extraFloatData1": 10.00,
                            "extraFloatData2": 23.45,
                            "extraStringData1": "ProgramName",
                            "extraStringData2": "NNYNNNN",
                            "extraDateData1": "2023-10-01",
                        },
                    ],
                }
