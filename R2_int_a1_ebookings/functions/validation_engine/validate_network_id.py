import logging
import boto3
import os
import json
import time
from boto3 import client as boto3_client
from functions.common_utils import CommonUtils
from swm_logger.swm_common_logger import LambdaLogger

custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])


def lambda_handler(event, context):
    """
    Funtion to validate the Network Id in BRQ file
    """
    custom_logger.info(f"event_data: {event}")
    common_utils = CommonUtils(event, custom_logger, context)
    if event["validationResult"]["continueValidation"] is True:
        validation_status = "SUCCESS"
        continue_validation = True
        custom_logger.info(f"event_data: {event}")
        s3 = boto3.resource("s3")
        key = event["brqFileName"] + ".json"
        s3_object = s3.Object(event["brqJsonPath"], key)
        file_content = s3_object.get()["Body"].read().decode("utf-8")
        json_content = json.loads(file_content)
        allowed_network_ids = ["PRIPRO", "SEVNET", "7QLD"]
        if json_content["header"]["NetworkId"] not in allowed_network_ids:
            custom_logger.info(f"BRQ [{event['brqId']}] is meant for another Network")
            recipients = [event["brqEmail"], "RevManOps@Seven.com.au"]
            subject = "The BRQ file received is meant for another Network."
            body = f"The BRQ file received is meant for another Network. Please refer to attached email and follow up with Agency/Direct Client."
            common_utils.send_email_notification(
                recipients, subject, body, event, [".eml"]
            )
            time.sleep(15)
            common_utils.move_file_s3_to_s3(
                os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
                os.environ["EBOOKINGS_S3_ERROR_BUCKET"],
                os.environ["EBOOKINGS_S3_FILEIN_BUCKET"],
                event["brqDirPath"],
                event["brqFileName"],
            )
            validation_status = "ERROR"
            continue_validation = False
        event["validationResult"]["continueValidation"] = continue_validation
        event["validationResult"]["result"] = validation_status
        event["validationResult"]["details"].append(
            {
                "ruleName": "Validate BRQ Network Id",
                "result": validation_status,
                "notificationTo": "",
                "msg": "",
            }
        )
    return event
