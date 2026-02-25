import logging
import boto3
import os
import json
import time
from boto3 import client as boto3_client
from functions.common_utils import CommonUtils
from datetime import datetime
from swm_logger.swm_common_logger import LambdaLogger

custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])

ARN_SF_ADAPTOR = os.environ["SALESFORCE_ADAPTOR"]
lambda_client = boto3_client("lambda", region_name=os.environ["SEIL_AWS_REGION"])


def get_account_details(event):
    """
    Function to fetch the B2B relationship details from salesforce.
    """
    sf_agency_account_id = event["sfAgencyAccountId"]
    sf_advertiser_account_id = event["sfAdvertiserAccountId"]
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Active__c,+SWM_End_Date__c,+SWM_Start_Date__c,+Account_Name__c,+Related_Account_Name__c+FROM+Account_Account_Relationship__c+WHERE+Relationship_Type__c+='Billing Account of'+AND+Account__c+='{sf_agency_account_id}'+AND+Related_Account__c+='{sf_advertiser_account_id}'+AND+Active__c+=True",
    }
    try:
        invoke_response = lambda_client.invoke(
            FunctionName=ARN_SF_ADAPTOR,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        downstream_response = json.loads(invoke_response["Payload"].read())
        custom_logger.info(downstream_response)
        return downstream_response
    except Exception as e:
        custom_logger.info(f"Error getting salesforce B2B data: {e}")
        return False


def get_case_description(event, account_data):
    description = []
    date_format = "%Y-%m-%d"
    if account_data["totalSize"] == 0:
        description.append(
            f"Agency [{event['brqAgencyName']}] 'Billing Account of’ Advertiser [{event['brqClientName']}] B2B Relationship does not exist"
        )
        custom_logger.info(
            f"BRQ [{event['brqId']}] - A 'Billing Account of' relationship does not exist"
        )
    if account_data["totalSize"] > 0:
        agency_account_name = account_data["records"][0]["Account_Name__c"]
        advertiser_account_name = account_data["records"][0]["Related_Account_Name__c"]
        if account_data["records"][0]["Active__c"] is not True:
            description.append(
                f"Agency [{agency_account_name}] 'Billing Account of' Advertiser [{advertiser_account_name}] is not an active B2B Relationship"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - The 'Billing Account of' relationship is not an active B2B Relationship"
            )
        if datetime.strptime(event["brqWcStartDate"], date_format) < datetime.strptime(
            account_data["records"][0]["SWM_Start_Date__c"], date_format
        ) or datetime.strptime(event["brqWcEndDate"], date_format) > datetime.strptime(
            account_data["records"][0]["SWM_End_Date__c"], date_format
        ):
            description.append(
                f"There is no active B2B Relationship for Billing Account '[{agency_account_name}] of Advertiser [{advertiser_account_name}] for the Campaign Dates."
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - The 'Billing Account of' relationship is not an active for the Campaign Dates"
            )
    return "\n".join(description)


def lambda_handler(event, context):
    """
    Function to validate B2B relationship with agency and advertiser client
    """
    common_utils = CommonUtils(event, custom_logger, context)
    if event["validationResult"]["continueValidation"] is True:
        custom_logger.info(f"event_data: {event}")
        validation_status = "SUCCESS"
        continue_validation = True
        account_data = get_account_details(event)
        date_format = "%Y-%m-%d"
        # ToDo - Campaign Date validation - Blocked due to no fields('SWM_Start_Date__c', 'SWM_End_Date__c') available on Sf B2B Object
        if account_data["totalSize"] > 0:
            if (
                account_data["records"][0]["Active__c"] is not True
                or datetime.strptime(event["brqWcStartDate"], date_format)
                < datetime.strptime(
                    account_data["records"][0]["SWM_Start_Date__c"], date_format
                )
                or datetime.strptime(event["brqWcEndDate"], date_format)
                > datetime.strptime(
                    account_data["records"][0]["SWM_End_Date__c"], date_format
                )
            ):
                description = get_case_description(event, account_data)
                # common_utils.move_file_s3_to_s3(
                #     os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
                #     os.environ["EBOOKINGS_S3_ERROR_BUCKET"],
                #     os.environ["EBOOKINGS_S3_FILEIN_BUCKET"],
                #     event["brqDirPath"],
                # )
                # common_utils.create_sf_case(event["brqFileName"], description, event)
                event["caseContent"].append("\n\n" + description)
                validation_status = "ERROR"
                continue_validation = False
        else:
            description = get_case_description(event, account_data)
            # common_utils.move_file_s3_to_s3(
            #     os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
            #     os.environ["EBOOKINGS_S3_ERROR_BUCKET"],
            #     os.environ["EBOOKINGS_S3_FILEIN_BUCKET"],
            #     event["brqDirPath"],
            # )
            # common_utils.create_sf_case(event["brqFileName"], description, event)
            event["caseContent"].append("\n\n" + description)
            validation_status = "ERROR"
            continue_validation = False
        custom_logger.info(f"SF B2B object response: {account_data}")
        if len(event["caseContent"]) > 0:
            description = "\n".join(event["caseContent"])
            common_utils.create_sf_case(event["brqFileName"], description, event)
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
                "ruleName": "Validate B2B relationship of Client ",
                "result": validation_status,
                "notificationTo": "",
                "msg": "",
            }
        )
    return event
