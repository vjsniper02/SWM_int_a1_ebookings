import boto3
import json
import os
import logging
from boto3 import client as boto3_client

from functions.s3_utils import save_to_s3, read_from_s3
from functions.a1_2.task_integration_job_spots_prebooking_api import (
    update_int_job_spots_loading,
)

AWS_REGION = os.environ["SEIL_AWS_REGION"]
ARN_SF_ADAPTOR = os.environ["SALESFORCE_ADAPTOR"]
lambda_client = boto3_client("lambda", region_name=AWS_REGION)
logger = logging.getLogger("a1_2_call_spot_prebooking_function")
logger.setLevel(logging.INFO)

# Clear existing handlers if they exist, to prevent duplicate logs from Lambda's default setup
# This step is crucial for custom formatting to be the only active one.
if not logger.handlers:
    ch = logging.StreamHandler()
    # Create a formatter without the correlation ID for now,
    # as correlation ID will be added later by the class instance
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

REGION_NAME = "ap-southeast-2"


def lambda_handler(event, context):
    handler = CallSpotPrebookingAPIHandler(event)
    response = handler.handle()

    # return event to pass to next step in the state machine
    return response


class CallSpotPrebookingAPIHandler:
    def __init__(self, event):
        self.event = event
        self.correlation_id = event["id"]
        self.opportunity_id = event["detail"]["sf_payload"]["sf"]["opportunityID"]
        self.brqRequestID = event["brqRequestID"]

        self.in_bucket_name = os.environ["EBOOKINGS_S3_FILEIN_BUCKET"]
        self.temp_bucket_name = os.environ["EBOOKINGS_S3_TEMP_BUCKET"]
        self.seil_config_bucket_name = os.environ["SEIL_CONFIG_BUCKET_NAME"]
        self.landmark_adaptor = os.environ["LANDMARK_ADAPTOR"]
        self.region = os.environ["SEIL_AWS_REGION"] or REGION_NAME

        self.brq_json_bucket = event["brqJsonBucket"]
        self.brq_json_key = event["brqJsonKey"]

        # Get the same logger instance as defined globally
        self.logger = logging.getLogger("a1_2_call_spot_prebooking_function")
        self.logger.setLevel(logging.INFO)  # Ensure level is set

        self.logger.info(
            f"{self.correlation_id} - The input event of the a1_2_call_spot_prebooking_payload.lambda_handler"
        )
        self.logger.info(f"{self.correlation_id} - {self.event}")

    def handle(self):
        if "campaignHeaderResponseCode" in self.event and (
            self.event["campaignHeaderResponseCode"] == 200
            or self.event["campaignHeaderResponseCode"] == 201
        ):
            payload = {
                "entity": "Opportunity",
                "invocationType": "SOBJECTS",
                "brqid": self.brqRequestID,
                "payload": {
                    "SWM_Error_Message__c": "E-booking spots are currently being loaded into the Pre-Book screen. Please wait until the 'E-Booking Spots created' field has been updated before continuing the process in Landmark.",
                },
            }
            self.update_sf_opportunity(payload, self.opportunity_id)

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
            
            spotPrebookingResponsePathList = []
            spotPrebookingResponseCodeList = []
            spotPrebookingStatusList = []

            # Loop through iteration count
            for i in range(iteration_count):
                print(payload_list)
                if i >= len(payload_list):
                    raise IndexError(f"Index {i} out of range for payload list")

                payload_value = payload_list[i]

                self.__invoke_landmark_adaptor(payload_value)
                status = self.__determine_status()
                response_body_json = json.loads(self.response_body)
                lmk_status = self.__get_downstream_status(response_body_json)
                if status == "QueueMessage":
                    spotPrebookingStatusList.append("QueueMessage")
                    spotPrebookingResponseCodeList.append(503)
                    self.event.update(
                        {
                            "spotPrebookingStage": True,
                            "spotPrebookingResponseCode": spotPrebookingResponseCodeList,
                            "spotPrebookingerror": "Landmark responded with 503, transaction has been queued",
                            "spotPrebookingStatus": spotPrebookingStatusList,
                        }
                    )
                    return self.event
                elif status == "failed":
                    file_path = self.__save_response_to_s3(i+1)
                    spotPrebookingResponsePathList.append(file_path)
                    spotPrebookingResponseCodeList.append(lmk_status)
                    spotPrebookingStatusList.append("failed")
                    self.event.update(
                        {
                            "spotPrebookingStatus": "failed",
                            "spotPrebookingStage": True,
                            "spotPrebookingResponsePath": spotPrebookingResponsePathList,
                            "spotPrebookingResponseCode": lmk_status,
                        }
                    )
                else:
                    file_path = self.__save_response_to_s3(i+1)
                    spotPrebookingResponsePathList.append(file_path)
                    spotPrebookingResponseCodeList.append(lmk_status)
                    spotPrebookingStatusList.append("success")
                    self.event.update(
                        {
                            "spotPrebookingResponseCode": lmk_status,
                            "spotPrebookingResponsePath": spotPrebookingResponsePathList,
                            "spotPrebookingStatus": "success",
                        }
                    )
                self.event.update(
                    {
                        "spot_iteration": i + 1,
                    }
                )

                update_int_job_spots_loading(self.event, "context")
            return self.event

    def update_sf_opportunity(self, payload, opp_id):
        payload["method"] = "UPDATE"
        payload["oppId"] = opp_id
        try:
            invoke_response = lambda_client.invoke(
                FunctionName=ARN_SF_ADAPTOR,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            self.logger.info(f" Opp Update response: {invoke_response}")
            return json.loads(invoke_response["Payload"].read())
        except Exception as e:
            self.logger.info(f" Error occured in Opp Update: {e}")
            return None

    # def __invoke_landmark_adaptor(self):
    #     client = boto3.client("lambda")

    #     # Get iteration count and payload list from event
    #     iteration_count = int(self.event.get("trancheFileCount", 0))
    #     payload_list = self.event.get("spotPayloadFilePath", [])

    #     if iteration_count <= 0 or not payload_list:
    #         raise ValueError("Invalid iteration count or empty payload list")

    #     # Loop through iteration count
    #     for i in range(iteration_count):
    #         if i >= len(payload_list):
    #             raise IndexError(f"Index {i} out of range for payload list")

    #         payload_value = payload_list[i]

    #         # Invoke downstream Lambda
    #         response = client.invoke(
    #             FunctionName=self.landmark_adaptor,
    #             Payload=json.dumps(
    #                 {
    #                     "operation": "post",
    #                     "landmarkEndpoint": "/api/v1/SpotPreBooking",
    #                     "content": payload_value
    #                 }
    #             ),
    #         )
    #         self.response = response
    #         self.response_body = self.response["Payload"].read().decode("utf-8").strip()
    #         self.logger.info(f"LMK Response: {self.response_body}")
    #         self.event.update(
    #             {
    #                 "spot_iteration": i+1,
    #             }
    #         )

    #         update_int_job_spots_loading(self.event,"context")
    #     if self.response_body == "503":
    #         self.logger.info(f"LMK Response type: {type(self.response_body)}")
    #     return self.response

    def __invoke_landmark_adaptor(self, payload_value):
        client = boto3.client("lambda")

        response = client.invoke(
            FunctionName=self.landmark_adaptor,
            Payload=json.dumps(
                {
                    "operation": "post",
                    "landmarkEndpoint": "/api/v1/SpotPreBooking",
                    "content": payload_value,
                }
            ),
        )
        # response will be 503 if LMK is unavailable
        self.response = response
        self.response_body = self.response["Payload"].read().decode("utf-8").strip()
        self.logger.info(f"LMK Response: {self.response_body}")
        if self.response_body == "503":
            self.logger.info(f"LMK Response type: {type(self.response_body)}")
        return self.response

    def __get_downstream_status(self, response_body_json):
        """
        Attempts to extract the status from landmark API response.
        """
        self.logger.info(
            f"{self.correlation_id} - Current raw response_body_json: {response_body_json}"
        )
        self.logger.info(
            f"{self.correlation_id} - Type of response_body_json: {type(response_body_json)}"
        )

        # --- Case 1: The response_body_json is a list of dictionaries ---
        if isinstance(response_body_json, list):
            if not response_body_json:  # Handle empty list
                self.logger.info(
                    f"{self.correlation_id} - Response body is an empty list. No status to extract."
                )
                return None

            # Iterate through each item (which should be a dictionary) in the list
            for i, item_dict in enumerate(response_body_json):
                self.logger.info(
                    f"{self.correlation_id} - Processing item {i} (Type: {type(item_dict)}): {item_dict}"
                )
                if isinstance(item_dict, dict):
                    # Attempt Format 1: Check for 'messages' array within this individual item_dict
                    try:
                        if "messages" in item_dict and isinstance(
                            item_dict["messages"], list
                        ):
                            messages_list = item_dict["messages"]
                            if messages_list:
                                for message_item in messages_list:
                                    if (
                                        isinstance(message_item, dict)
                                        and "status" in message_item
                                    ):
                                        status_from_message = message_item["status"]
                                        self.logger.info(
                                            f"{self.correlation_id} - Found status from messages array in item {i}: {status_from_message}"
                                        )
                                        return status_from_message  # Return the first status found

                    except (KeyError, TypeError) as e:
                        self.logger.info(
                            f"{self.correlation_id} -  Item {i}: Format 1 check failed or 'messages' structure invalid: {e}. Trying Format 2 for this item..."
                        )
                        pass

                    # Attempt Format 2: Check for 'status' at the top-level of this individual item_dict
                    status_at_top_level_in_item = item_dict.get("status")
                    if status_at_top_level_in_item is not None:
                        self.logger.info(
                            f"{self.correlation_id} -  Item {i}: Found status at top-level of item: {status_at_top_level_in_item}"
                        )
                        return status_at_top_level_in_item
                else:
                    self.logger.info(
                        f"{self.correlation_id} -  Item {i} is not a dictionary. Skipping status extraction for this item."
                    )

            # If we've gone through all items in the list and found no status
            self.logger.info(
                f"{self.correlation_id} - No status found in any dictionary within the response list."
            )
            return None

        # --- Case 2: The response_body_json is a single dictionary (original intended logic) ---
        elif isinstance(response_body_json, dict):
            self.logger.info(
                f"{self.correlation_id} - Response body is a single dictionary. Applying original logic..."
            )
            # Attempt Format 1: Check for 'messages' array at top level
            try:
                if "messages" in response_body_json and isinstance(
                    response_body_json["messages"], list
                ):
                    messages_list = response_body_json["messages"]
                    if messages_list:
                        for message_item in messages_list:
                            if (
                                isinstance(message_item, dict)
                                and "status" in message_item
                            ):
                                status_from_message = message_item["status"]
                                self.logger.info(
                                    f"{self.correlation_id} - Found status from messages array (top-level dict): {status_from_message}"
                                )
                                return status_from_message
                    else:
                        self.logger.info(
                            "  'messages' array is empty (top-level dict). Skipping Format 1."
                        )

            except (KeyError, TypeError) as e:
                self.logger.info(
                    f"{self.correlation_id} -  Format 1 check failed or 'messages' structure invalid (top-level dict): {e}. Trying Format 2..."
                )
                pass

            # Attempt Format 2: Check for 'status' at the top-level of the main dictionary
            status_at_top_level = response_body_json.get("status")
            if status_at_top_level is not None:
                self.logger.info(
                    f"{self.correlation_id} -  Found status at top-level of dictionary: {status_at_top_level}"
                )
                return status_at_top_level

        # --- Case 3: The response_body_json is neither a list nor a dictionary ---
        else:
            self.logger.info(
                f"{self.correlation_id} - Unexpected response_body_json type: {type(response_body_json)}. Expected list or dict."
            )

        return None  # Default return if no status is found in any format

    def __determine_status(self):
        if self.response_body == "503":
            return "QueueMessage"

        self.logger.info(self.response_body)
        self.response_body_json = json.loads(self.response_body)
        response_body_json = json.loads(self.response_body)
        lmk_status = self.__get_downstream_status(response_body_json)
        if lmk_status != 200:
            if lmk_status != 201:
                return "failed"

        if lmk_status == 200 or lmk_status == 201:
            return "success"
        return "status unknown"

    def __save_response_to_s3(self, index):
        save_to_s3(
            Body=self.response_body,
            Bucket=self.temp_bucket_name,
            Key=f"{self.correlation_id}/spots_response_{index}.json",
        )
        return f"s3://{self.temp_bucket_name}/{self.correlation_id}/spots_response_{index}.json"
