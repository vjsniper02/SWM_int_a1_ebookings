import boto3
import logging
import os
import json
import re
import csv
import io
import math
import copy
from datetime import timedelta, datetime as dt
import datetime

from functions.a1_2.SalesAreaMap import SalesAreaMap
from functions.simple_table import (
    sum_by,
    group_by_count,
    group_by_sum,
    min_list,
    max_list,
    sanitize_delivery_length,
    sanitize_strike_weight_list,
)


def lambda_handler(event, context):
    """
    The event is
    {
        "opportunityID": "string, mandatory",
        "RecordTypeDeveloperName": "string, mandatory, the Salesforce RecordType Developer to indicate EBooking,Fixed or Dynamic",
        "BRQRequestID": "string, mandatory",
        "correlationID": "string, optional. If this is empty, use the EventBridge Event ID",

        # the following two properties are from get_brq_file.py which is the previous step in
        # the Ebooking Spots Preprocessing State Machine
        "brqJsonBucket": "string, mandatory",
        "brqJsonKey": "string, mandatory"
    }

    This lambda prepares the campaign header information like sales area split, strike weight, day parts and delivery length.

    Since the payload could be big, the payload is saved in S3 bucket and the event will have an additional
    property `campaignHeaderPayloadPath` in format `s3://{bucket_name}/{file_key}`.
    """

    handler = PrepareCampaignHeaderPayloadHandler(event)
    handler_output = handler.handle()

    # merge the handler_output with the event for next steps
    event.update(handler_output)

    # return event to pass to next step in the state machine
    return event


class PrepareCampaignHeaderPayloadHandler:
    def __init__(self, event):
        self._param_name = os.environ["LMK_DAYPART_ID"]
        self.DAY_PART_ID = self.__get_path_by_param_name()
        self.event = copy.deepcopy(
            event
        )  # make a copy of event, because we need to remove some attributes from the event object. Search "pop" in this file.
        self.correlation_id = event["id"]  # Event ID
        self.opportunity_id = event["detail"]["sf_payload"]["sf"]["opportunityID"]
        self.campaign_code = event["detail"]["campaign_code"]

        self.in_bucket_name = os.environ["EBOOKINGS_S3_FILEIN_BUCKET"]
        self.temp_bucket_name = os.environ["EBOOKINGS_S3_TEMP_BUCKET"]
        self.seil_config_bucket_name = os.environ["SEIL_CONFIG_BUCKET_NAME"]
        self.sales_area_mapping_path = os.environ["SEIL_SALES_AREA_MAPPING_PATH"]
        self.region = os.environ["SEIL_AWS_REGION"]

        self.brq_json_bucket = event["brqJsonBucket"]
        self.brq_json_key = event["brqJsonKey"]

        # prepare the logger with correlation ID
        self.logger = logging.getLogger(
            "prepare_campaign_header_payload.lambda_handler"
        )
        self.logger.__format__ = logging.Formatter(
            self.correlation_id + " - %(message)s"
        )

        self.logger.info(
            "The input event of the prepare_campaign_header_payload.lambda_handler"
        )
        self.logger.info(self.event)

        self.lmk_campaign_dict = (
            {}
        )  # key = parentSalesArea, value = one object in salesAreaOnCampaigns

    def handle(self):
        self.__read_brq_json()
        self.__prepare_sales_area_dict()
        self.__group_by_parent_sales_area()
        self.__calculate_campaign()
        self.__fixed_campaign_header()
        file_path = self.__save_to_s3()
        return {"campaignHeaderPayloadPath": file_path}

    # @property
    # def landmark_campaign_payload(self):
    #     # Change from Dictionary to List which is the Landmark campaign list required.
    #     self.event["campaign"]["salesAreaOnCampaigns"] = [self.lmk_campaign_dict[x] for x in self.lmk_campaign_dict]
    #     return {
    #         "approvalID": 1,
    #         "approvalSourceID": 0,
    #         "safeMode": False,
    #         "campaigns": [
    #             self.event["campaign"]
    #         ]
    #     }

    def __get_path_by_param_name(self):
        client = boto3.client("ssm")
        response = client.get_parameter(Name=self._param_name, WithDecryption=True)

        if "Parameter" not in response or response["Parameter"] == None:
            raise Exception(
                f"Parameter does not exist, The parameter '{self._param_name}' must exist in AWS Parameter Store."
            )

        return response["Parameter"]["Value"]

    def __fixed_campaign_header(self):
        payload = self.event["detail"]["sf_payload"]
        campaign = self.event["detail"]["sf_payload"]["campaigns"][0]

        # set numberOfSpots and add additional fields
        campaign["numberOfSpots"] = len(self.brq_object["details"])
        campaign["campaignCode"] = self.campaign_code
        campaign["salesAreaOnCampaignAsPerPolicy"] = False
        campaign["daypartsAsPerDeal"] = False

        # The below two for loop is to merge the existing SalesArea list and the calculated SalesArea list
        # First For Loop
        #
        # 1. If existing SalesArea is not found in the calculated SalesArea, remove from the  existing SalesArea
        # 2. If existing SalesArea is found in the calculated SalesArea, replace the existing SalesArea by the calculated SalesArea
        #
        # Second For Loop
        # 3. If calculated SalesArea is not found in the existing SalesArea, append to the SalesAreaDetails list
        #
        index = -1
        for salesarea in campaign["salesAreaOnCampaigns"]:
            index += 1
            if salesarea["salesAreaNumber"] not in self.lmk_campaign_dict:
                # Remove the salesarea from the payload if it is not found in the calcualted SalesArea.
                campaign["salesAreaOnCampaigns"].remove(salesarea)

                # # set all percentage to zero
                # salesarea["percentageSplit"] = 0
                # salesarea["deliveryCurrencyPricing"]["deliveryCurrencyType"] = "NumberOfSpots"
                # salesarea["deliveryCurrencyPricing"]["deliveryCurrencyPriceValue"] = 0
                # for sales_detail in salesarea["salesAreaDetails"]:
                #     sales_detail["spotsPercentage"] = 0
                #     sales_detail["percentageSplit"] = 0
            else:
                # replace the element
                new_salesarea = self.lmk_campaign_dict[salesarea["salesAreaNumber"]]
                campaign["salesAreaOnCampaigns"][index] = new_salesarea
                self.lmk_campaign_dict.pop(salesarea["salesAreaNumber"], None)

        for new_salesareanumber in self.lmk_campaign_dict:
            new_salesarea = self.lmk_campaign_dict[new_salesareanumber]
            campaign["salesAreaOnCampaigns"].append(new_salesarea)

        # remove unnecessary key
        payload.pop("sf", None)
        payload.pop("integrationJobID", None)
        payload.pop("opportunityID", None)
        payload.pop("processingStateMachineArn", None)
        payload.pop("spotLength", None)

        self.lmk_upload_campaign_payload = payload

    def __read_brq_json(self):
        s3client = boto3.client("s3", region_name=self.region)
        s3object = s3client.get_object(
            Bucket=self.brq_json_bucket, Key=self.brq_json_key
        )
        file_content = s3object["Body"].read()
        brq_object = json.loads(file_content.decode("utf-8"))

        self.brq_object = brq_object

        return brq_object

    def __prepare_sales_area_dict(self):
        self.sales_area_map = SalesAreaMap(sales_area_path=self.sales_area_mapping_path)

        # sa_dict for fast search
        sa_dict = {}  # key is the salesAreaNumber
        for area in self.sales_area_map.data:
            # sa_dict contains both BCC and salesAreaNumber key
            if "BCC" in area and area["BCC"]:
                sa_dict[area["BCC"]] = area
            sa_dict[area["salesAreaNumber"]] = (
                area  # support both BCC and salesAreaNumber
            )

        self.sa_dict = sa_dict

    def __group_by_parent_sales_area(self):
        if not hasattr(self, "sa_dict"):
            self.__prepare_sales_area_dict()

        merged_list = []
        grouped = {}  # key is the parentSalesArea, value is a list of BRQ rows
        overall_grouped = {}

        for row in self.brq_object["details"]:
            stationId = row["StationId"]
            salesArea = self.sa_dict[stationId]
            parentArea = (
                salesArea["Overall_ParentSalesAreaNumber"]
                if salesArea["Overall_ParentSalesAreaNumber"]
                else salesArea["salesAreaNumber"]
            )
            if parentArea in grouped:
                grouped[parentArea].append(row)
            else:
                grouped[parentArea] = [row]

        # Merge arrays(spot lenght data) for all keys(sales areas)
        # to make common across overall campaign - Fix as part SIT issue 2416
        for key in grouped:
            merged_list.extend(grouped[key])

        # Update all keys(sales areas) with the merged list
        for key in grouped:
            overall_grouped[key] = merged_list

        # print(f"grouped : {grouped}")
        # print(f"overall_grouped : {overall_grouped}")
        self.brq_grouped_by_parent_sales_area = grouped
        self.brq_grouped_by_overall_parent_sales_area = overall_grouped

    def __calculate_campaign(self):
        # use self.brq_object to calcualte sales area
        # and set to self.lmk_campaign_dict
        # Refer to https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138/A1+-+eBooking+Detailed+Design#Feature-8.2---Campaign-Header-Calculation
        # for the documentation

        remaining = 100
        for i, parent_area in enumerate(self.brq_grouped_by_parent_sales_area):
            one_parent_sales_area_details = self.brq_grouped_by_parent_sales_area[
                parent_area
            ]
            overall_parent_sales_area_details = (
                self.brq_grouped_by_overall_parent_sales_area[parent_area]
            )

            if i == len(self.brq_grouped_by_parent_sales_area) - 1:
                percentage = remaining
            else:
                percentage = (
                    math.floor(
                        len(one_parent_sales_area_details)
                        / len(self.brq_object["details"])
                        * 1000000
                    )
                    / 10000
                )  # based on the number of spots
                remaining = round(
                    remaining - percentage, 4
                )  # Need to round it as 33.34 - 11.11 = 22.230000000000004
            # print(f"DEBUG: percentageSplit: {percentage}")
            if parent_area not in self.lmk_campaign_dict:
                self.lmk_campaign_dict[int(parent_area)] = {
                    "salesAreaNumber": int(parent_area),
                    "percentageSplit": percentage,
                    "deliveryCurrencyPricing": {
                        "deliveryCurrencyType": "NumberOfSpots",
                        "deliveryCurrencyPriceValue": len(
                            one_parent_sales_area_details
                        ),
                    },
                }

            one_parent_sales_area = self.lmk_campaign_dict[int(parent_area)]

            one_parent_sales_area["salesAreaDetails"] = (
                self.__calculate_sales_area_for_one_parent_sales_area(
                    parent_area, one_parent_sales_area_details, percentage
                )
            )
            one_parent_sales_area["deliveryLengths"] = (
                self.__calculate_delivery_length_for_one_parent_sales_area(
                    one_parent_sales_area_details, overall_parent_sales_area_details
                )
            )
            one_parent_sales_area["dayparts"] = (
                self.__calculate_day_parts_for_one_parent_sales_area(
                    one_parent_sales_area_details
                )
            )
            one_parent_sales_area["strikeWeights"] = (
                self.__calculate_strike_weigth_for_one_parent_sales_area(
                    one_parent_sales_area_details
                )
            )

            self.lmk_campaign_dict[int(parent_area)] = one_parent_sales_area

    def __calculate_sales_area_for_one_parent_sales_area(
        self,
        parent_sales_area_number,
        one_parent_sales_area_details,
        parent_area_percentage,
    ):
        # use self.brq_object to calcualte "salesAreaDetails"
        # and set to self.lmk_campaign_dict
        # Refer to https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138/A1+-+eBooking+Detailed+Design#Feature-8.2---Campaign-Header-Calculation
        # for the documentation
        # calculate SalesArea

        if not hasattr(self, "sa_dict"):
            self.__prepare_sales_area_dict()

        percentage_base = len(
            self.brq_object["details"]
        )  # Based on Scott's email, Sales Area percentage is based on the whole campaign. (https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138?focusedCommentId=306577666)

        grouped_by_stationId = group_by_count(
            one_parent_sales_area_details, "StationId"
        )
        salesAreaDetails = []
        used_sales_areas = []
        remaining = parent_area_percentage
        for i, stationId in enumerate(grouped_by_stationId):
            if i == len(grouped_by_stationId) - 1:
                percentage = remaining
                # print(
                #     f"LAST PERCENTAGE of Sales Area = {parent_area_percentage}, {percentage}"
                # )
            else:
                percentage = (
                    math.floor(
                        grouped_by_stationId[stationId] / percentage_base * 1000000
                    )
                    / 10000
                )
                remaining = round(
                    remaining - percentage, 4
                )  # Need to round it as 33.34 - 11.11 = 22.230000000000004
            #     print(
            #         f"PERCENTAGE of Sales Area = {parent_area_percentage}, {remaining}, {percentage}"
            #     )
            # print(f"DEBUG: spotsPercentage: {percentage}")
            salesAreaDetails.append(
                {
                    "salesAreaNumber": int(self.sa_dict[stationId]["salesAreaNumber"]),
                    "isExcluded": False,
                    "percentageSplit": percentage,
                    "spotsPercentage": percentage,
                }
            )
            used_sales_areas.append(self.sa_dict[stationId]["salesAreaNumber"])

        # Add remaining SalesAreas although they are 0%
        for area in self.sales_area_map.data:
            if (
                area["Overall_ParentSalesAreaNumber"] == parent_sales_area_number
                and area["salesAreaNumber"] not in used_sales_areas
            ):
                salesAreaDetails.append(
                    {
                        "salesAreaNumber": int(area["salesAreaNumber"]),
                        "isExcluded": False,
                        "percentageSplit": 0,
                    }
                )

        return salesAreaDetails

    def __calculate_strike_weigth_for_one_parent_sales_area(
        self, one_parent_sales_area_details
    ):
        # use self.brq_object to calcualte "strikeWeights"
        # and set to self.lmk_campaign_dict
        # Refer to https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138/A1+-+eBooking+Detailed+Design#Feature-8.2---Campaign-Header-Calculation
        # for the documentation

        min_wc_date = min_list(one_parent_sales_area_details, "WCDate")
        max_wc_date = max_list(one_parent_sales_area_details, "WCDate")

        # min_date = dt.strptime(min, '%Y%m%d')
        # max_date = dt.strptime(max, '%Y%m%d') + timedelta(days=6)

        strike_weight_list = []
        # Convert the date strings to datetime objects
        startDate = dt.strptime(min_wc_date, "%Y%m%d")
        startDate = dt.strftime(startDate, "%Y-%m-%d")
        startDate = dt.strptime(startDate, "%Y-%m-%d")
        endDate = dt.strptime(max_wc_date, "%Y%m%d")
        endDate = dt.strftime(endDate, "%Y-%m-%d")
        endDate = dt.strptime(endDate, "%Y-%m-%d") + timedelta(days=6)

        # Function to find the next Saturday from a given date
        def next_saturday(date):
            return date + timedelta(days=(5 - date.weekday() + 7) % 7)

        # Initialize variables
        split_dates = []
        current_date = startDate
        self.logger.info(current_date)

        # Split the date range
        while current_date <= endDate:
            split_start_date = current_date
            split_end_date = min(next_saturday(current_date), endDate)
            spot_count_given_dates = sum(
                1
                for obj in one_parent_sales_area_details
                if split_start_date
                <= dt.strptime(obj["WCDate"], "%Y%m%d")
                <= split_end_date
            )
            split_dates.append(
                (split_start_date, split_end_date, spot_count_given_dates)
            )
            current_date = next_saturday(current_date) + timedelta(days=1)

        # Calculate the percentage for each split date range
        total_days = (endDate - startDate).days + 1
        total_ratings_percent = 0
        total_spots_percent = 0

        for start_date, end_date, spot_count_for_week in split_dates:
            num_days = (end_date - start_date).days + 1
            ratings_percent = math.floor((num_days / total_days) * 100)
            spots_percent = math.floor(
                (spot_count_for_week / len(one_parent_sales_area_details)) * 100
            )
            total_ratings_percent += ratings_percent
            total_spots_percent += spots_percent
            # self.logger.info(f"Split: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}, Percentage: {percent:.2f}%")
            strike_weight_item = {
                "period": {
                    "startDate": start_date.strftime("%Y-%m-%d"),
                    "endDate": end_date.strftime("%Y-%m-%d"),
                },
                "ratingsPercentage": ratings_percent,
                "spotsPercentage": spots_percent,
            }
            strike_weight_list.append(strike_weight_item)

        if total_ratings_percent < 100:
            strike_length = len(strike_weight_list)
            strike_weight_list[strike_length - 1]["ratingsPercentage"] = (
                strike_weight_list[strike_length - 1]["ratingsPercentage"]
                + (100 - total_ratings_percent)
            )

        if total_spots_percent < 100:
            strike_length = len(strike_weight_list)
            strike_weight_list[strike_length - 1]["spotsPercentage"] = (
                strike_weight_list[strike_length - 1]["spotsPercentage"]
                + (100 - total_spots_percent)
            )
        # print(f"Strike weight before normalization : {strike_weight_list}")
        strike_weight_list = sanitize_strike_weight_list(
            strike_weight_list, len(one_parent_sales_area_details)
        )
        # print(f"Strike weight post normalization : {strike_weight_list}")
        return strike_weight_list

    def __calculate_delivery_length_for_one_parent_sales_area(
        self, one_parent_sales_area_details, overall_parent_sales_area_details
    ):
        # use self.brq_object to calcualte "deliveryLengths"
        # and set to self.lmk_campaign_dict
        # Refer to https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138/A1+-+eBooking+Detailed+Design#Feature-8.2---Campaign-Header-Calculation
        # for the documentation

        # Based on Scott's email, Delivery Length percentage is based on the Target Sales Area. (https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138?focusedCommentId=306577666)
        percentage_base = len(one_parent_sales_area_details)

        requested_sizes = group_by_count(one_parent_sales_area_details, "RequestedSize")

        delivery_lengths = []
        remaining = 100.0
        for i, requested_size in enumerate(requested_sizes):
            if i == len(requested_sizes) - 1:
                percentage = remaining
            else:
                percentage = (
                    math.floor(
                        requested_sizes[requested_size] / percentage_base * 1000000
                    )
                    / 10000
                )
                remaining = round(
                    remaining - percentage, 4
                )  # Need to round it as 33.34 - 11.11 = 22.230000000000004
            # print(f"DEBUG: percentage: {percentage}")
            delivery_lengths.append(
                {"spotLength": int(requested_size), "percentage": percentage}
            )
        delivery_lengths = sanitize_delivery_length(
            overall_parent_sales_area_details, delivery_lengths, "RequestedSize"
        )
        # print(f"delivery_lengths : {delivery_lengths}")
        return delivery_lengths

    def __calculate_day_parts_for_one_parent_sales_area(
        self, one_parent_sales_area_details
    ):
        # use self.brq_object to calcualte "dayparts"
        # and set to self.lmk_campaign_dict
        # Refer to https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138/A1+-+eBooking+Detailed+Design#Feature-8.2---Campaign-Header-Calculation
        # for the documentation

        # Based on the comment: https://code7plus.atlassian.net/browse/R2DEV-1395
        # SEIL will have one single day parts

        return [
            {
                # Based on Scott's email, Day Parts percentage is based on the Target Sales Area. (https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138?focusedCommentId=306577666)
                # Since we have only 1 day part, it is always 100%
                "percentage": 100,
                "daypartNameID": self.DAY_PART_ID,
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

    def __save_to_s3(self):
        """
        Save the payload to S3 bucket.
        """
        data = self.lmk_upload_campaign_payload
        file_key = self.correlation_id + "/campaign_header_payload.json"
        s3client = boto3.client("s3", region_name=self.region)
        s3client.put_object(
            Body=json.dumps(data), Bucket=self.temp_bucket_name, Key=file_key
        )
        return f"s3://{self.temp_bucket_name}/{file_key}"
