import logging
import boto3
import os
import json
import time
from boto3 import client as boto3_client
from functions.common_utils import CommonUtils
from swm_logger.swm_common_logger import LambdaLogger


custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])
CEE_NOTIFICATION_ENGINE = os.environ["CEE_NOTIFICATION_ENGINE"]
step_function = boto3_client("stepfunctions", region_name=os.environ["SEIL_AWS_REGION"])


def find_demo_percentage(part, whole):
    percentage = 100 * float(part) / float(whole)
    return percentage


def get_invalid_demo_count(event):
    count = 0
    s3 = boto3.resource("s3")
    key = event["brqFileName"] + ".json"
    s3_object = s3.Object(event["brqJsonPath"], key)
    file_content = s3_object.get()["Body"].read().decode("utf-8")
    json_content = json.loads(file_content)

    total_spot_count = len(json_content["details"])
    for item in json_content["details"]:
        if (
            item["DemographicCodeOne"] == ""
            and item["DemographicCodeTwo"] == ""
            and item["DemographicCodeThree"] == ""
            and item["DemographicCodeFour"] == ""
        ):
            count += 1
    return count, total_spot_count


def lambda_handler(event, context):
    """
    Funtion to validate the Demo Tolerance in BRQ file
    """
    custom_logger.info(f"event_data: {event}")
    common_utils = CommonUtils(event, custom_logger, context)
    if event["validationResult"]["continueValidation"] is True:
        validation_status = "SUCCESS"
        continue_validation = True
        demo_tolerance = common_utils.get_ssm_parameter(
            os.environ["DEMO_TOLERANCE_PERCENTAGE"]
        )
        invalid_demo_count, total_spot_count = get_invalid_demo_count(event)
        demo_percentage = find_demo_percentage(invalid_demo_count, total_spot_count)
        if demo_percentage >= float(demo_tolerance):
            custom_logger.info(
                f"BRQ [{event['brqId']}] has spots missing Demo above permitted tolerance"
            )
            recipients = [event["brqEmail"]]
            subject = "BRQ has Spots missing Demo above permitted tolerance."
            body = f"The number of spots in the BRQ file missing the demographic is above the permitted tolerance. Please refer to attached email and follow up with Agency/Direct Client. Each spot in the BRQ file should have the primary demographic assigned."
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
        event["validationResult"]["result"] = validation_status
        event["validationResult"]["continueValidation"] = continue_validation
        event["validationResult"]["details"].append(
            {
                "ruleName": "Validate demo tolerance",
                "result": validation_status,
                "notificationTo": "",
                "msg": "",
            }
        )
    return event
