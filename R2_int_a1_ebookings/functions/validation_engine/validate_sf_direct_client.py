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
        "query": f"SELECT+Name,+Id,+SWM_LandMark_ID__c,+RecordTypeId,+Type,+SWM_Trading_Type__c,+vlocity_cmt__Status__c,+Credit_Status__c+FROM+Account+WHERE+SWM_External_Account_ID__c+='{client_id}'",
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


def get_case_description(event, account_data, recordtype_data={}):
    description = []
    if account_data["totalSize"] == 0:
        description.append(
            f"BRQ [{event['brqClientId']}, {event['brqClientName']}] - Direct Client did not match to a Direct Client Advertiser Account in Salesforce"
        )
        custom_logger.info(
            f"BRQ [{event['brqId']}] - Direct Client did not match to a Direct Client Advertiser Account in Salesforce"
        )
    if account_data["totalSize"] > 0:
        account_name = account_data["records"][0]["Name"]
        account_credit_status = account_data["records"][0]["Credit_Status__c"]
        account_type = account_data["records"][0]["Type"]
        if recordtype_data["records"][0]["Name"] != "Advertiser":
            description.append(
                f"A Salesforce Account [{account_name}] was found but it is not an Advertiser"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - A Direct Client Salesforce Account was found but it is not an Advertiser"
            )
        if account_data["records"][0]["Type"] not in [
            "Direct Client",
            "Agency Client & Direct Client",
        ]:
            description.append(
                f"Invalid Advertiser Type [{account_type}] for Advertiser [{account_name}]"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - Salesforce Account does not have a Type of 'Advertiser'"
            )
        if account_data["records"][0]["vlocity_cmt__Status__c"] != "Active":
            description.append(
                f"Salesforce Account [{account_name}] is not an Active Account"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] -Direct Client Advertiser Salesforce Account is not an Active Account"
            )
        if account_data["records"][0]["SWM_Trading_Type__c"] not in [
            "Broadcast",
            "Broadcast & Digital",
        ]:
            description.append(
                f"Salesforce Account [{account_name}] is not set up for Broadcast Trading"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - Advertiser Salesforce Account is not set up for Broadcast Trading"
            )
        if account_data["records"][0]["Credit_Status__c"] in [
            "Prepay",
            "Credit Not Allowed",
        ]:
            description.append(
                f"Invalid Credit Status [{account_credit_status}] for Direct Client [{account_name}]"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - Invalid Credit Status for Direct Client"
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
        account_data_for_clientid = get_account_details(
            event["brqClientId"]
        )
        if account_data_for_clientid["totalSize"] > 0:
            event["sfAdAgAccountLandmarkId"] = account_data_for_clientid["records"][0][
                "SWM_LandMark_ID__c"
            ]
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
            validation_status = "ERROR"
            continue_validation = False
        event["validationResult"]["result"] = validation_status
        event["validationResult"]["continueValidation"] = continue_validation
        event["validationResult"]["details"].append(
            {
                "ruleName": "Validate SF Direct Client ",
                "result": validation_status,
                "notificationTo": "",
                "msg": "",
            }
        )
    return event
