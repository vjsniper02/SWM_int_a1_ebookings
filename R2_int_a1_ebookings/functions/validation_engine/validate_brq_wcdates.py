import logging
import boto3
import os
import json
import time
from boto3 import client as boto3_client
from datetime import date, datetime, timedelta
from functions.common_utils import CommonUtils
from swm_logger.swm_common_logger import LambdaLogger

custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])

CEE_NOTIFICATION_ENGINE = os.environ["CEE_NOTIFICATION_ENGINE"]
step_function = boto3_client("stepfunctions", region_name=os.environ["SEIL_AWS_REGION"])


def sanitize_date_format(wc_date):
    dateTimeObj = datetime.strptime(wc_date, "%Y%m%d")
    wc_date = dateTimeObj.date()
    wc_date = wc_date.strftime("%Y-%m-%d")
    return wc_date


def get_current_week_sunday():
    today = date.today()
    days_to_last_sunday = (today.weekday() + 1) % 7
    last_sunday = today - timedelta(days=days_to_last_sunday)
    return last_sunday


def validate_wcdates(event):
    count = 0
    s3 = boto3.resource("s3")
    key = event["brqFileName"] + ".json"
    s3_object = s3.Object(event["brqJsonPath"], key)
    file_content = s3_object.get()["Body"].read().decode("utf-8")
    json_content = json.loads(file_content)

    # Adding additional required attributes in event response to avopid BRQ file reading operation everytime.
    # Next step functions on the validation flow can access these attributes.
    event["brqAgencyId"] = json_content["header"]["AgencyId"]
    event["brqAgencyName"] = json_content["header"]["AgencyName"]
    event["brqClientId"] = json_content["details"][0]["ClientId"]
    event["brqClientName"] = json_content["details"][0]["ClientName"]
    event["brqNetworkName"] = json_content["header"]["NetworkId"]
    event["brqClientProductId"] = json_content["details"][0]["ClientProductId"]
    event["brqClientProductName"] = json_content["details"][0]["ClientProductName"]

    current_sunday = get_current_week_sunday()
    current_sunday = datetime.strptime(f"{current_sunday}", "%Y-%m-%d")
    if current_sunday > datetime.strptime(event["brqWcStartDate"], "%Y-%m-%d"):
        event["validationMessages"].append(
            "The Campaign Start Date has been amended as the file contains spots in previous weeks"
        )
    for item in json_content["details"]:
        wc_date = sanitize_date_format(item["WCDate"])
        wc_date = datetime.strptime(f"{wc_date}", "%Y-%m-%d")
        if current_sunday > wc_date:
            count += 1
    if count > 0:
        if len(json_content["details"]) == count:
            custom_logger.info(f"All wc_dates validation failed: {count}")
            return False
    custom_logger.info(f"wc_date validation passed: {count}")
    return True


def lambda_handler(event, context):
    """
    Funtion to validate the Demo Tolerance in BRQ file
    """
    custom_logger.info(f"event_data: {event}")
    common_utils = CommonUtils(event, custom_logger, context)
    if event["validationResult"]["continueValidation"] is True:
        validation_status = "SUCCESS"
        continue_validation = True
        if validate_wcdates(event) is False:
            custom_logger.info(
                f"BRQ [{event['brqId']}] contains w/c dates all in the past"
            )
            validation_status = "ERROR"
            continue_validation = False
            recipients = [event["brqEmail"]]
            subject = "BRQ File is in the past"
            body = f"The BRQ sent through contains w/c dates that are all in the past. Please refer to attached email and follow up with Agency/Direct Client"
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
            custom_logger.info(
                f"BRQ [BRQ Request ID] contains w/c dates all in the past"
            )
        if len(event["errored_sales_area_list"]) > 0:
            errored_sales_area_list = event["errored_sales_area_list"]
            validation_status = "ERROR"
            continue_validation = False
            custom_logger.info(
                f"Sales area code validation failure occured, creating case in salesforce"
            )
            errored_sa_values = "".join(errored_sales_area_list)
            description = f"Station Id  Station Name\n\n {errored_sa_values}"
            common_utils.create_sf_case_SA_notmatch(
                event["brqFileName"], description, event
            )
            time.sleep(15)
            common_utils.move_file_s3_to_s3(
                os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
                os.environ["EBOOKINGS_S3_ERROR_BUCKET"],
                os.environ["EBOOKINGS_S3_FILEIN_BUCKET"],
                event["brqDirPath"],
                event["brqFileName"],
            )
        event["validationResult"]["result"] = validation_status
        event["validationResult"]["continueValidation"] = continue_validation
        event["validationResult"]["details"].append(
            {
                "ruleName": "Validate WC dates",
                "result": validation_status,
                "notificationTo": "",
                "msg": "",
            }
        )
        custom_logger.info(f"Response - Updated event data: {event}")
    return event
