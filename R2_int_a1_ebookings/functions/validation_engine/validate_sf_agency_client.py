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


def get_account_details_for_clientid(client_id):
    """
    Function to fetch the account details from salesforce matching 'Client Id' from the BRQ file.
    """
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Name,+Id,+SWM_LandMark_ID__c,+RecordTypeId,+Type,+SWM_Trading_Type__c,+vlocity_cmt__Status__c,+Credit_Status__c+from+Account+WHERE+SWM_External_Account_ID__c+='{client_id}'",
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
        custom_logger.info(f"Error getting salesforce Account data for client Id: {e}")
        return False


def get_account_details(agency_id):
    """
    Function to fetch the account details from salesforce matching 'Agency Id' from the BRQ file.
    """
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Name,+Id,+SWM_LandMark_ID__c,+RecordTypeId,+Type,+SWM_Trading_Type__c,+vlocity_cmt__Status__c,+Credit_Status__c+from+Account+WHERE+(SWM_External_Account_ID__c+='{agency_id}'+OR+SWM_Additional_External_Ids__c+='{agency_id}'+OR+SWM_Additional_External_Ids__c+LIKE+'{agency_id};%'+OR+SWM_Additional_External_Ids__c+LIKE+'%;{agency_id};%'+OR+SWM_Additional_External_Ids__c+LIKE+'%;{agency_id}')+AND+RecordType.Name+='Agency'",
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
            f"The BRQ [{event['brqAgencyId']}, {event['brqAgencyName']}] did not match to an Agency Account in Salesforce"
        )
        custom_logger.info(
            f"BRQ [{event['brqId']}] - Agency did not match to an Agency Account in Salesforce"
        )
    if account_data["totalSize"] > 0:
        account_name = account_data["records"][0]["Name"]
        account_credit_status = account_data["records"][0]["Credit_Status__c"]
        # if recordtype_data["records"][0]["Name"] != "Agency":
        #     description.append(
        #         f"A Salesforce Account [{account_name}] was found but it is not an Agency"
        #     )
        #     custom_logger.info(
        #         f"BRQ [{event['brqId']}] - A Salesforce Account was found but it is not an Agency"
        #     )
        if account_data["records"][0]["vlocity_cmt__Status__c"] != "Active":
            description.append(
                f"Salesforce Account [{account_name}] is not an Active Account’"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - Agency Salesforce Account is not an Active Account"
            )
        if account_data["records"][0]["SWM_Trading_Type__c"] not in [
            "Broadcast",
            "Broadcast & Digital",
        ]:
            description.append(
                f"Salesforce Account [{account_name}] is not set up for Broadcast Trading"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - Agency Salesforce Account is not set up for Broadcast Trading"
            )
        if account_data["records"][0]["Type"] != "Agency":
            description.append(
                f"Salesforce Account [{account_name}] is not an ‘Agency’"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - Salesforce Account does not have a Type of 'Agency'"
            )
        if account_data["records"][0]["Credit_Status__c"] in [
            "Prepay",
            "Credit Not Allowed",
        ]:
            description.append(
                f"Invalid Credit Status [{account_credit_status}] for Agency [{account_name}]"
            )
            custom_logger.info(
                f"BRQ [{event['brqId']}] - Invalid Credit Status for Agency"
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
    Function to validate salesforce agency client
    """
    common_utils = CommonUtils(event, custom_logger, context)
    if event["validationResult"]["continueValidation"] is True:
        custom_logger.info(f"event_data: {event}")
        validation_status = "SUCCESS"
        continue_validation = True
        account_data = get_account_details(event["brqAgencyId"])
        account_data_for_clientid = get_account_details_for_clientid(
            event["brqClientId"]
        )
        if account_data_for_clientid["totalSize"] > 0:
            event["sfAdAccountLandmarkId"] = account_data_for_clientid["records"][0][
                "SWM_LandMark_ID__c"
            ]
        if account_data["totalSize"] > 0:
            event["sfAccountLandmarkId"] = account_data["records"][0][
                "SWM_LandMark_ID__c"
            ]
            custom_logger.info(f"SF account object response: {account_data}")
            event["sfAgencyAccountId"] = account_data["records"][0]["Id"]
            if (
                account_data["records"][0]["Type"] != "Agency"
                or account_data["records"][0]["vlocity_cmt__Status__c"] != "Active"
                or account_data["records"][0]["SWM_Trading_Type__c"]
                not in ["Broadcast", "Broadcast & Digital"]
                or account_data["records"][0]["Credit_Status__c"]
                in ["Prepay", "Credit Not Allowed"]
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
        else:
            custom_logger.info(f"No SF account object data found: {account_data}")
            description = get_case_description(event, account_data)
            if len(event["caseContent"]) > 0:
                event["caseContent"].append("\n\n" + description)
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
                "ruleName": "Validate SF Agency Client",
                "result": validation_status,
                "notificationTo": "",
                "msg": "",
            }
        )
    return event
