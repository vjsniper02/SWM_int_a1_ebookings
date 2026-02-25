import logging
import boto3
import os
import json
import time
from boto3 import client as boto3_client
from functions.common_utils import CommonUtils
from swm_logger.swm_common_logger import LambdaLogger

custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])

ARN_SF_ADAPTOR = os.environ["SALESFORCE_ADAPTOR"]
lambda_client = boto3_client("lambda", region_name=os.environ["SEIL_AWS_REGION"])


def get_account_details(client_id):
    """
    Function to fetch the account details from salesforce matching 'Client Id' from the BRQ file.
    """
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Name,+Id,+SWM_LandMark_ID__c,+RecordTypeId,+Type,+SWM_Trading_Type__c,+vlocity_cmt__Status__c,+Credit_Status__c+FROM+Account+WHERE+SWM_External_Account_ID__c+='{client_id}'+AND+RecordType.Name+='Advertiser'",
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
        custom_logger.info(f"Error getting salesforce Account data: {e}")
        return False


def get_case_description(event, account_data):
    description = []
    if account_data["totalSize"] == 0:
        description.append(
            f"Advertiser [{event['brqClientId']}, {event['brqClientName']}] from the BRQ did not match to an Advertiser Account in Salesforce"
        )
        custom_logger.info(
            f"BRQ [{event['brqFileName']}] - Advertiser did not match to an Advertiser Account in Salesforce"
        )
    if account_data["totalSize"] == 1:
        account_name = account_data["records"][0]["Name"]
        account_type = account_data["records"][0]["Type"]
        account_credit_status = account_data["records"][0]["Credit_Status__c"]
        # Commented out due to the updates as part of R2TC-2849
        #
        # if recordtype_data["records"][0]["Name"] != "Advertiser":
        #     description.append(
        #         f"A Salesforce Account [{account_name}] was found but it is not an Advertiser"
        #     )
        #     custom_logger.info(
        #         f"BRQ [{event['brqFileName']}] - A Salesforce Account was found but it is not an Advertiser"
        #     )
        if account_data["records"][0]["Type"] not in [
            "Agency Client",
            "Direct Client",
            "Agency Client & Direct Client",
        ]:
            description.append(
                f"Invalid Advertiser Type [{account_type}] for Advertiser [{account_name}]"
            )
            custom_logger.info(
                f"BRQ [{event['brqFileName']}] - Salesforce Account does not have a Type of 'Advertiser'"
            )
        if account_data["records"][0]["vlocity_cmt__Status__c"] != "Active":
            description.append(
                f"Salesforce Account [{account_name}] is not an Active Account"
            )
            custom_logger.info(
                f"BRQ [{event['brqFileName']}] -Advertiser Salesforce Account is not an Active Account"
            )
        if account_data["records"][0]["SWM_Trading_Type__c"] not in [
            "Broadcast",
            "Broadcast & Digital",
        ]:
            description.append(
                f"Salesforce Account [{account_name}] is not set up for Broadcast Trading"
            )
            custom_logger.info(
                f"BRQ [{event['brqFileName']}] - Advertiser Salesforce Account is not set up for Broadcast Trading"
            )
        if account_data["records"][0]["Credit_Status__c"] in [
            "Prepay",
            "Credit Not Allowed",
        ] and account_data["records"][0]["Type"] in ["Direct Client"]:
            description.append(
                f"Invalid Credit Status [{account_credit_status}] for Direct Client   [{account_name}]"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - Invalid Credit Status for Direct Client"
            )
    if account_data["totalSize"] > 1:
        description.append(
            f"Multiple Advertiser Salesforce Accounts were found for BRQ Advertiser [{event['brqClientId']}]. Please investigate."
        )
        custom_logger.info(
            f"[{event['brqFileName']}] - Multiple Advertiser Salesforce Accounts were found for BRQ Advertiser [{event['brqClientId']}]."
        )
    return "\n".join(description)


def get_recordtype_details(recordtype_id):
    """
    Function to fetch the recordtype information for the recordtypeId supplied in argument.
    """
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Id+,+Name+FROM+RecordType+WHERE+Id+=+'{recordtype_id}'",
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
        custom_logger.info(f"Error getting salesforce recordtype data: {e}")
        return False


def lambda_handler(event, context):
    """
    Function to validate salesforce agency advertiser client
    """
    common_utils = CommonUtils(event, custom_logger, context)
    if event["validationResult"]["continueValidation"] is True:
        custom_logger.info(f"event_data: {event}")
        validation_status = "SUCCESS"
        continue_validation = True
        account_data = get_account_details(event["brqClientId"])
        if account_data["totalSize"] == 1:
            custom_logger.info(f"SF account object response: {account_data}")
            event["sfAdvertiserAccountId"] = account_data["records"][0]["Id"]
            event["sfAdvertiserAccountName"] = account_data["records"][0]["Name"]
            if (
                account_data["records"][0]["Type"]
                not in [
                    "Agency Client",
                    "Direct Client",
                    "Agency Client & Direct Client",
                ]
                or account_data["records"][0]["vlocity_cmt__Status__c"] != "Active"
                or account_data["records"][0]["SWM_Trading_Type__c"]
                not in ["Broadcast", "Broadcast & Digital"]
                or (
                    account_data["records"][0]["Credit_Status__c"]
                    in ["Prepay", "Credit Not Allowed"]
                    and account_data["records"][0]["Type"] in ["Direct Client"]
                )
            ):
                description = get_case_description(event, account_data)
                # common_utils.move_file_s3_to_s3(
                #     os.environ["EBOOKINGS_S3_FILEIN_BUCKET"],
                #     os.environ["EBOOKINGS_S3_ERROR_BUCKET"],
                #     event["brqDirPath"],
                # )
                # common_utils.create_sf_case(event["brqFileName"], description, event)
                event["caseContent"].append("\n\n" + description)
                validation_status = "ERROR"
                continue_validation = True
        elif account_data["totalSize"] > 1:
            description = get_case_description(event, account_data)
            common_utils.create_sf_case(event["brqFileName"], description, event)
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
        else:
            custom_logger.info(f"No SF account object data found: {account_data}")
            description = get_case_description(event, account_data)
            common_utils.create_sf_case(event["brqFileName"], description, event)
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
                "ruleName": "Validate SF Agency Adertiser Client ",
                "result": validation_status,
                "notificationTo": "",
                "msg": "",
            }
        )
    return event
