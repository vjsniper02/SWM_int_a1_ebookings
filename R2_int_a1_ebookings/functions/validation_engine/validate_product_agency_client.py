import logging
import boto3
import os
import json
from boto3 import client as boto3_client
from functions.common_utils import CommonUtils
from swm_logger.swm_common_logger import LambdaLogger

custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])


def lambda_handler(event, context):
    """
    Funtion to validate product agency client in LMK
    """
    common_utils = CommonUtils(event, custom_logger, context)
    if event["validationResult"]["continueValidation"] is True:
        custom_logger.info(f"event_data: {event}")
        validation_status = "SUCCESS"
        continue_validation = True
        sf_landmark_id = event.get("sfAccountLandmarkId", "")
        landmark_payer_code = event["lmkProductResponse"].get("payerCode", "")
        if (
            "payerCode" in event["lmkProductResponse"]
            and sf_landmark_id != ""
            and sf_landmark_id.casefold() != landmark_payer_code.casefold()
        ):
            description = f"Agency [{event['brqAgencyName']}] is not the Payer allocated on the Product [{event['lmkProductResponse']['productName']}]. The Biller allocated is [{event['lmkProductResponse']['payerName']}]"
            validation_status = "WARNING"
            title = "BRQ Agency not Billing Agency on Product Case"
            first_line = "This EBooking Case has been raised due to the Billing Agency on the matched Product not being the Agency Id on the BRQ Product"
            case_response = common_utils.create_sf_case_product_notmatch(
                event["brqFileName"],
                description,
                event,
                first_line,
                title,
            )
            if (
                "id" in case_response
                and case_response["id"]
                and case_response["success"] is True
            ):
                event["validationMessages"].append(
                    f"The Ebooking Billing Agency does not match the Product’s Billing Agency. A case has been raised with Sales Insights. They will reach out to you via Chatter to proceed and resolve"
                )
            event["validationResult"]["result"] = validation_status
            event["validationResult"]["continueValidation"] = continue_validation
            event["validationResult"]["details"].append(
                {
                    "ruleName": "Validate Product agency client in LMK",
                    "result": validation_status,
                    "notificationTo": "",
                    "msg": "",
                }
            )
    return event
