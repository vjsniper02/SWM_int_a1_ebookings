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
    Funtion to validate product advertiser in LMK
    """
    common_utils = CommonUtils(event, custom_logger, context)
    if event["validationResult"]["continueValidation"] is True:
        custom_logger.info(f"event_data: {event}")
        validation_status = "SUCCESS"
        continue_validation = True
        sf_landmark_id = event.get("sfAdAccountLandmarkId", "")
        landmark_advertiser_code = event["lmkProductResponse"].get("advertiserCode", "")
        if (
            "advertiserCode" in event["lmkProductResponse"]
            and sf_landmark_id != ""
            and sf_landmark_id.casefold() != landmark_advertiser_code.casefold()
        ):
            description = f"Advertiser Account [{event['brqClientName']}] is not the Advertiser on the Product [{event['lmkProductResponse']['productName']}]."
            validation_status = "WARNING"
            event["validationMessages"].append(
                f"Advertiser Account [{event['brqClientName']}] is not the Advertiser on the Product [{event['lmkProductResponse']['productName']}]."
            )
            title = "SF Advertiser for EBooking does not match Product"
            first_line = "This EBooking Case has been raised due to the SF Advertiser not being the Advertiser on the BRQ Product"
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
                    "A Product Not Matched Case has been raised and assigned to Sales Insights. They will reach out to you to ensure they have all the necessary information to create the product."
                )
            event["validationResult"]["result"] = validation_status
            event["validationResult"]["continueValidation"] = continue_validation
            event["validationResult"]["details"].append(
                {
                    "ruleName": "Validate Product advertiser in LMK",
                    "result": validation_status,
                    "notificationTo": "",
                    "msg": "",
                }
            )
    return event
