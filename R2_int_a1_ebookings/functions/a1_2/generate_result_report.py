import os
import json
import re
import csv
import io
from functions.s3_utils import save_to_s3, read_from_s3
from datetime import datetime


"""
Field Mapping: Key=Title in the Report; Value=Field Name in the BRQ object
"""
BRQ_OBJECT_FIELD_MAPPING = {
    "Agency Name": "AgencyName",
    "Client Name": "ClientName",
    "Client Product Name": "ClientProductName",
    "Station Name": "StationName",
    "W/C Date": "WCDate",
    "Days": "RequestedDay",
    "Time": "RequestedTime",
    "Program": "RequestedProgram",
    "Duration": "RequestedSize",
    "Rate": "RequestedGrossRate",
    "Demographic Code One": "DemographicCodeOne",
    "Demographic One Tarp": "DemographicOneTarp",
    "Demographic One Thousand": "DemographicOneThousand",
    "Proposed Agency Spot ID": "UniqueAgencyProposedSpotId",
    "Booking Modifiers": "BookingModifiers",
}


def lambda_handler(event, context):
    # The report detail is documented in the JIRA ticket: https://code7plus.atlassian.net/browse/R2DEV-1758
    # https://code7plus.atlassian.net/browse/R2DEV-1185
    # return event to pass to next step in the state machine

    generator = ResultReportGenerator(event)
    result = generator.generate()

    event.update(result)

    return event


class ResultReportGenerator:
    def __init__(self, event):
        self.event = event
        self.spotPrebookingResponsePath = event["spotPrebookingResponsePath"]
        self.brqJsonBucket = event["brqJsonBucket"]
        self.brqJsonKey = event["brqJsonKey"]
        self.correlation_id = event["id"]
        self.opportunity_id = event["detail"]["sf_payload"]["sf"]["opportunityID"]
        self.campaign_code = event["detail"]["campaign_code"]
        self.approvalID = str(event["detail"]["sf_payload"]["approvalID"])

        self.in_bucket_name = os.environ["EBOOKINGS_S3_FILEIN_BUCKET"]
        self.temp_bucket_name = os.environ["EBOOKINGS_S3_TEMP_BUCKET"]

    def generate(self):
        # read brq json
        brq_json_content = read_from_s3(self.brqJsonBucket, self.brqJsonKey)
        self.brq_json = json.loads(brq_json_content)

        # Get iteration count and payload list from event
        iteration_count = int(self.event.get("trancheFileCount", 1))
        # Get the value from the event
        payload_value = self.event.get("spotPayloadFilePath", [])

        # Ensure it's always a list
        if isinstance(payload_value, str):
            payload_list = [payload_value]
        elif isinstance(payload_value, list):
            payload_list = payload_value
        else:
            payload_list = []  # fallback if it's neither string nor list
        response_list = self.event.get("spotPrebookingResponsePath", [])

        spotPrebookingReportlist = []
        spotPrebookingResultStatusList = []
        spotPrebookingResultFileNameToSF = []
        self.overall_report_json = []
        self.cur_iteration_report_json = []
        # Loop through iteration count
        for i in range(iteration_count):
            if i >= len(payload_list):
                raise IndexError(f"Index {i} out of range for payload list")

            payload_value = payload_list[i]
            response_value = response_list[i]
            payload_s3_path = payload_value
            bucket_prefix = "s3://" + self.temp_bucket_name + "/"
            if payload_s3_path.startswith(bucket_prefix):
                payload_key = payload_s3_path[len(bucket_prefix) :]
            else:
                payload_key = payload_s3_path

            response_s3_path = response_value
            bucket_prefix = "s3://" + self.temp_bucket_name + "/"
            if response_s3_path.startswith(bucket_prefix):
                response_key = response_s3_path[len(bucket_prefix) :]
            else:
                response_key = response_s3_path

            # read spot payload json
            spot_payload_content = read_from_s3(self.temp_bucket_name, payload_key)
            self.spot_payload_json = json.loads(spot_payload_content)

            # read spot response json
            spot_response_content = read_from_s3(self.temp_bucket_name, response_key)
            print(spot_response_content)
            self.spot_response_json = json.loads(spot_response_content)

            # there are two types of spot_response_json, one is object, ons is array
            if isinstance(self.spot_response_json, dict):
                print(self.spot_response_json)
                self.status_detail, self.overall_status = self.__dict_to_status_detail(
                    self.spot_response_json
                )
            elif isinstance(self.spot_response_json, list):
                self.status_detail, self.overall_status = self.__list_to_status_detail(
                    self.spot_response_json
                )
            else:
                raise Exception("Spot response must be either object or array.")

            # Glue status_detail with brq content
            self.cur_iteration_report_json = self.__glue_with_brq_content()
            self.overall_report_json += self.__glue_with_brq_content()

            # generate csv file
            self.__generate_csv()

            # save to s3
            file_path = self.__save_to_s3(i + 1)
            spotPrebookingReportlist.append(file_path)
            spotPrebookingResultStatusList.append(self.overall_status)
            spotPrebookingResultFileNameToSF.append(self.report_file_name)
        return {
            "spotPrebookingReport": spotPrebookingReportlist,
            "spotPrebookingResultStatus": spotPrebookingResultStatusList,
            "spotPrebookingResultFileNameToSF": spotPrebookingResultFileNameToSF,
        }

    def __construct_file_name(self, index) -> str:
        # Refer to https://code7plus.atlassian.net/browse/R2DEV-1758
        # Updated for ticket - https://code7plus.atlassian.net/browse/R2DEV-2501
        # file name = SpotsNotLoadedinLMK-12345678-00002197-PEARMA-2024-01-30_101659
        file_name = (
            "SpotFailure-"
            + self.event["brqRequestID"]
            + "-OP"
            + self.approvalID
            + "-"
            + datetime.now().strftime("%Y-%m-%d")
            + "-"
            + str(index)
            + ".csv"
        )
        return file_name

    def __save_to_s3(self, index) -> str:
        """
        Save the report to S3 and return the report path in format "s3://{bucket}/{file_key}".
        """
        file_name = self.__construct_file_name(index)
        self.report_file_name = file_name

        file_key = f"{self.correlation_id}/{file_name}"

        print("self.csv_content")
        print(self.csv_content)
        save_to_s3(self.csv_content.encode("utf-8"), self.temp_bucket_name, file_key)
        self.report_file_uri = f"s3://{self.temp_bucket_name}/{file_key}"
        return self.report_file_uri

    def __glue_with_brq_content(self):
        result = []
        for index in self.status_detail:
            brq_object = self.brq_json["details"][index]

            one_report_entity = {}
            for report_title in BRQ_OBJECT_FIELD_MAPPING:
                field_name = BRQ_OBJECT_FIELD_MAPPING[report_title]
                if field_name == "AgencyName":
                    one_report_entity[report_title] = self.__get_from_header(
                        "AgencyName"
                    )
                elif field_name == "BookingModifiers":
                    one_report_entity[report_title] = "".join(
                        brq_object["BookingModifiers"]
                    )
                else:
                    one_report_entity[report_title] = brq_object[field_name]

            one_report_entity.update(
                {
                    "title": self.status_detail[index]["title"],
                    "status": self.status_detail[index]["status"],
                    "detail": self.status_detail[index]["msg"],
                }
            )
            result.append(one_report_entity)

        self.report_json = result
        print("self.report_json")
        print(json.dumps(self.report_json))
        return self.report_json

    def __generate_csv(self):
        titles = [report_title for report_title in BRQ_OBJECT_FIELD_MAPPING] + [
            "title",
            "status",
            "detail",
        ]
        csv_stream = io.StringIO()
        writer = csv.DictWriter(
            csv_stream, titles, delimiter=",", quoting=csv.QUOTE_NONNUMERIC
        )
        writer.writeheader()
        writer.writerows(self.cur_iteration_report_json)

        self.csv_content = csv_stream.getvalue()

        return self.csv_content

    def __list_to_status_detail(self, spot_response_list):
        """
        return a dict
        {
            0: {"title": "", "status": "200", "msg": "detail messages"},
            1: {"title": "", "status": "200", "msg": "detail messages"},
        }
        where 0 and 1 is the index in the spot payload json
        """

        """
        spot_response_list is in the following format:
        [
            {
                "campaignNumber": 278,
                "lineNumber": 1,
                "messages": [
                    {
                        "type": "urn:lmks:bll:23774",
                        "title": "Validation/Save failed",
                        "status": 422,
                        "detail": "Schedule Date cannot be outside the Campaign date range"
                    },
                    {
                        "type": "urn:lmks:bll:23778",
                        "title": "Validation/Save failed",
                        "status": 422,
                        "detail": "Business Type Code does not exists or not valid for Campaign."
                    }...
                ]
            }, ...
        ]
        """

        hasFailed = False
        hasSuccess = False
        result = {}
        for item in spot_response_list:
            line_number = item["lineNumber"]
            # match the line_number in self.spot_payload
            index = -1
            for payload_detail in self.spot_payload_json["spotPreBookingDetails"]:
                index += 1
                if payload_detail["lineNumber"] == line_number:
                    break
            status = 0
            msg = ""
            title = ""
            for message in item["messages"]:
                if message["status"] > 299:
                    hasFailed = True
                    if message["status"] > status:
                        status = message["status"]
                    msg += message["detail"] + ";"
                    title += message["title"] + ";"

                    result[index] = {
                        "title": title[0:-1],  # [0:-1] is to trim the final ';'
                        "status": status,
                        "msg": msg[0:-1],  # [0:-1] is to trim the final ';'
                    }
                else:
                    hasSuccess = True

        # This will be the value of SWM_Spots_created__c finally. It is either "Yes", "No", "Partial"
        if hasFailed and hasSuccess:
            self.overall_status = "Partial"
        elif hasSuccess:
            self.overall_status = "Yes"
        else:
            self.overall_status = "No"

        return result, self.overall_status

    def __dict_to_status_detail(self, spot_response_dict):
        """
        return a dict
        {
            0: {"title": "", "status": "200", "msg": "detail messages"},
            1: {"title": "", "status": "200", "msg": "detail messages"},
        }
        where 0 and 1 is the index in the spot payload json
        """

        """
        The dictionary looks like this
        {
            "SpotPreBookingDetails[0].SpotSalesAreaCode": [
                "The field SpotSalesAreaCode must be a string with a minimum length of 2 and a maximum length of 2."
            ],
            "SpotPreBookingDetails[0].BreakSalesAreaCode": [
                "The BreakSalesAreaCode field is required."
            ],
            "SpotPreBookingDetails[1].SpotSalesAreaCode": [
                "The field SpotSalesAreaCode must be a string with a minimum length of 2 and a maximum length of 2."
            ],...
        }
        """
        result = {}
        print(spot_response_dict)
        for key in spot_response_dict:
            matches = re.search(r"SpotPreBookingDetails\[(\d+)\]\.(.+)", key)
            if not matches:
                raise Exception(
                    "'"
                    + key
                    + r"' does not match regular expression 'SpotPreBookingDetails\[(\d+)\]\.(.+)'"
                )
            index = matches[1]
            field_name = matches[2]

            msg = f"{field_name}: {spot_response_dict[key]}"

            if index in result:
                result[index]["status"] = 400
                result[index]["msg"] += "\n" + msg
            else:
                result[index] = {"title": "Validation Error", "status": 400, "msg": msg}

        self.overall_status = "No"
        return result, "No"

    def __get_from_header(self, field_name):
        return self.brq_json["header"][field_name]
