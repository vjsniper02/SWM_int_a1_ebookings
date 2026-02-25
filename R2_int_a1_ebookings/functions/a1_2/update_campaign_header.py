import json
import logging
import boto3
import os
from botocore.exceptions import ClientError
import re
from functions.s3_utils import read_from_s3, save_to_s3

logger = logging.getLogger("a1_2_update_campaign_header_function")
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

REGION = "ap-southeast-2"


def lambda_handler(event, context):
    # connect to LMK
    handler = UpdateCampaignHeaderHandler(event)
    result = handler.handle()
    event.update(result)

    # return event to pass to next step in the state machine
    return event


class UpdateCampaignHeaderHandler:
    def __init__(self, event) -> None:
        self.event = event
        self.correlation_id = event["id"]
        self.opportunity_id = event["detail"]["sf_payload"]["sf"]["opportunityID"]

        self.in_bucket_name = os.environ["EBOOKINGS_S3_FILEIN_BUCKET"]
        self.temp_bucket_name = os.environ["EBOOKINGS_S3_TEMP_BUCKET"]
        self.seil_config_bucket_name = os.environ["SEIL_CONFIG_BUCKET_NAME"]
        self.sales_area_mapping_path = os.environ["SEIL_SALES_AREA_MAPPING_PATH"]
        self.region = os.environ["SEIL_AWS_REGION"]
        self.landmark_adaptor = os.environ["LANDMARK_ADAPTOR"]

        self.brq_json_bucket = event["brqJsonBucket"]
        self.brq_json_key = event["brqJsonKey"]

        self.error_from_message = ""
        # Get the same logger instance as defined globally
        self.logger = logging.getLogger("a1_2_update_campaign_header_function")
        self.logger.setLevel(logging.INFO)  # Ensure level is set
        # Get the same logger instance as defined globally
        self.logger = logging.getLogger("a1_2_update_campaign_header_function")
        self.logger.setLevel(logging.INFO)  # Ensure level is set

        self.logger.info(
            f"{self.correlation_id} - The input event of the prepare_campaign_header_payload.lambda_handler"
            f"{self.correlation_id} - The input event of the prepare_campaign_header_payload.lambda_handler"
        )
        self.logger.info(f"{self.correlation_id} - {self.event}")
        self.logger.info(f"{self.correlation_id} - {self.event}")

    def handle(self):
        self.logger.info(f"{self.correlation_id} - Processing campaign header update.")
        self.logger.info(f"{self.correlation_id} - Processing campaign header update.")
        self.__invoke_landmark_adaptor()
        status = self.__determine_status()
        response_body_json = json.loads(self.response_body)
        lmk_status = self.__get_downstream_status(response_body_json)
        response_body_json = json.loads(self.response_body)
        if status == "QueueMessage":
            self.event.update(
                {
                    "campaignHeaderStage": True,
                    "campaignHeaderResponseCode": 503,
                    "campaignHeadererror": "Landmark responded with 503, transaction has been queued",
                    "status": "QueueMessage",
                }
            )
            return self.event
        elif status == "failed":
            response_file_path = self.__save_response_to_s3()
            self.event.update(
                {
                    "campaignHeaderStage": True,
                    "campaignHeaderResponseCode": lmk_status,
                    "campaignHeadererror": self.error_from_message,
                    "status": "failed",
                    "campaignHeaderResponsePath": response_file_path,
                }
            )
            return self.event
        elif status == "success":
            response_file_path = self.__save_response_to_s3()
            self.event.update(
                {
                    "campaignHeaderResponseCode": lmk_status,
                    "campaignHeaderResponsePath": response_file_path,
                    "status": "success",
                }
            )
            return self.event
        else:
            response_file_path = self.__save_response_to_s3()
            self.event.update(
                {
                    "campaignHeaderStage": True,
                    "campaignHeaderResponseCode": lmk_status,
                    "campaignHeadererror": self.error_from_message,
                    "status": "status unknown",
                    "campaignHeaderResponsePath": response_file_path,
                }
            )
            return self.event

    def __get_downstream_status(self, response_body_json):
        """
        Attempts to extract the status from a downstream API response,
        prioritizing the 'messages' array, then a top-level 'status' key.
        """

        # --- Attempt Format 1: response_body_json["messages"] array ---
        try:
            if "messages" in response_body_json and isinstance(
                response_body_json["messages"], list
            ):
                messages_list = response_body_json["messages"]

                if messages_list:  # Check if the list is not empty
                    for message_item in messages_list:
                        if isinstance(message_item, dict) and "status" in message_item:
                            status_from_message = message_item["status"]
                            # ToDo - Have to loop the entire loop and check if status is 422 and set message, only first message can be captured or that has to be decided.
                            self.error_from_message = response_body_json[
                                "uploadCampaignResults"
                            ][0]["messages"][0]["detail"]
                            print(
                                f"  - Found LMK status from messages array: {status_from_message}"
                            )
                            return status_from_message  # Return the first status found in messages
                else:
                    print("LMK 'messages' array is empty. Skipping Format 1.")

        except (KeyError, TypeError) as e:
            # This catches if 'messages' is missing, or not a list, or items aren't dicts
            print(
                f"Format 1 check failed or 'messages' structure invalid: {e}. Trying Format 2..."
            )
            pass  # Continue to the next check if Format 1 fails

        # --- Attempt Format 2: response_body_json["status"] (top-level) ---
        # Use .get() to safely check for the 'status' key at the top level
        try:
            status_at_top_level = response_body_json.get("status")
        except:
            return response_body_json
        if status_at_top_level is not None:
            print(f"Status (Format 2): {status_at_top_level}")
            return status_at_top_level  # Return if found

    def __determine_status(self):
        if self.response_body == "503":
            return "QueueMessage"

        response_body_json = json.loads(self.response_body)
        # response_body_json = {
        #     "uploadCampaignResults": [
        #         {
        #             "approvalKeyID": 34310,
        #             "campaignCode": 28042,
        #             "campaignStatus": "UNCHANGED",
        #             "campaignIntendedAction": "AMEND",
        #             "messages": [
        #                 {
        #                     "type": "urn:lmks:web:214",
        #                     "title": "Validation/Save failed",
        #                     "status": 422,
        #                     "detail": "Campaign is locked.",
        #                 },
        #                 {
        #                     "type": "urn:lmks:web:301",
        #                     "title": "Validation/Save failed",
        #                     "status": 422,
        #                     "detail": "(Amend) Validation Failed",
        #                 },
        #             ],
        #         }
        #     ],
        #     "messages": [
        #         {
        #             "type": "urn:lmks:web:301",
        #             "title": "Validation/Save failed",
        #             "status": 422,
        #             "detail": "(Amend) Validation Failed",
        #         }
        #     ],
        # }
        lmk_status = self.__get_downstream_status(response_body_json)
        if lmk_status != 200:
            if lmk_status != 201:
                return "failed"
        if lmk_status == 200 or lmk_status == 201:
            return "success"
        return "status unknown"

    def __invoke_landmark_adaptor(self):
        client = boto3.client("lambda")

        response = client.invoke(
            FunctionName=self.landmark_adaptor,
            Payload=json.dumps(
                {
                    "operation": "post",
                    "landmarkEndpoint": "/api/v1/UploadCampaign",
                    "content": self.event["campaignHeaderPayloadPath"],
                }
            ),
        )
        self.response = response
        self.response_body = self.response["Payload"].read().decode("utf-8").strip()
        self.logger.info(f"LMK Response: {self.response_body}")
        if self.response_body == "503":
            self.logger.info(f"LMK Response type: {type(self.response_body)}")
        return self.response

    def __save_response_to_s3(self):
        save_to_s3(
            Body=json.dumps(self.response_body),
            Bucket=self.temp_bucket_name,
            Key=f"{self.correlation_id}/campaign_header_response.json",
        )
        full_key = f"s3://{self.temp_bucket_name}/{self.correlation_id}/campaign_header_response.json"
        return full_key
