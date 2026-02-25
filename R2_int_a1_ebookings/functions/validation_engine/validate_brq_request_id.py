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


def get_latest_opp(brq_id):
    lambda_client = boto3_client("lambda")
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Id,+AccountId,+SWM_Opportunity_Brief_ID__c,+StageName,+SWM_Status__c,+SWM_Start_Date__c,+SWM_End_Date__c,+SWM_No_of_BRQ_Demographics__c,+SWM_Demographic_Number__c,+SWM_Demographic_Name__c,+SWM_Landmark_Product_ID__c,+SWM_Landmark_Product_Name__c,+SWM_Number_of_Spots__c,+SWM_Overall_Budget__c,+SWM_Geography__c,+SWM_Duration__c,+SWM_No_of_BRQ_Spot_Lengths__c+FROM+Opportunity+WHERE+SWM_BRQ_Request_ID__c+='{brq_id}'+ORDER+BY+CreatedDate+DESC+LIMIT+1",
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
        custom_logger.info(f"Error getting salesforce Opp data: {e}")
        return False


def get_integration_job_status(opp_id):
    lambda_client = boto3_client("lambda")
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Opportunity__c,+Status__c+FROM+Integration_Job__c+WHERE+Opportunity__c+='{opp_id}'+ORDER+BY+CreatedDate+DESC+LIMIT+1",
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
        custom_logger.info(f"Error getting salesforce Integration Job data: {e}")
        return False


def lambda_handler(event, context):
    """
    Funtion to validate BRQ id duplication in SF
    """
    custom_logger.info(f"event_data: {event}")
    common_utils = CommonUtils(event, custom_logger, context)
    if event["validationResult"]["continueValidation"] is True:
        validation_status = "SUCCESS"
        continue_validation = True
        opp_data = get_latest_opp(event["brqId"])
        recipients = [event["brqEmail"]]
        subject = "BRQ Rejected"
        if opp_data["totalSize"] > 0:
            integration_status = get_integration_job_status(
                opp_data["records"][0]["Id"]
            )
            if opp_data["records"][0]["StageName"] in [
                "Campaign Header Created(won)",
                "Campaign Booked",
                "Superseded",
                "Closed Lost",
            ]:
                custom_logger.info(
                    f"BRQ [{event['brqId']}] was rejected due being associated with an existing Opportunity synced to Landmark or been set to 'Closed Lost''"
                )
                opp_acc_id = opp_data["records"][0]["AccountId"]
                stage_name = opp_data["records"][0]["StageName"]
                body = f"The BRQ file received has the same file name as an Opportunity [{opp_data['records'][0]['SWM_Opportunity_Brief_ID__c']}] that has been sync’d to Landmark or been set to 'Closed Lost'. Please refer to attached email and follow up with Agency/Direct Client."
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
            else:
                if integration_status["totalSize"] == 0:
                    event["oppStage"] = opp_data["records"][0]["StageName"]
                    event["oppId"] = opp_data["records"][0]["Id"]
                    event["oppData"] = opp_data["records"][0]
                else:
                    if integration_status["records"][0]["Status__c"] in [
                        "Unavailable",
                        "House Keeping",
                        "In Progress",
                    ]:
                        custom_logger.info(
                            f"BRQ [{event['brqId']}] was rejected due being associated with an existing Opportunity synced to Landmark or been set to 'Closed Lost''"
                        )
                        opp_acc_id = opp_data["records"][0]["AccountId"]
                        stage_name = opp_data["records"][0]["StageName"]
                        body = f"The BRQ file received has the same file name as an Opportunity [{opp_data['records'][0]['SWM_Opportunity_Brief_ID__c']}] that has been sync’d to Landmark or been set to 'Closed Lost'. Please refer to attached email and follow up with Agency/Direct Client."
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
                    else:
                        if integration_status["records"][0]["Status__c"] == "Failed":
                            event["oppStage"] = opp_data["records"][0]["StageName"]
                            event["oppId"] = opp_data["records"][0]["Id"]
        else:
            validation_status = "SUCCESS"
            continue_validation = True
        event["validationResult"]["result"] = validation_status
        event["validationResult"]["continueValidation"] = continue_validation
        event["validationResult"]["details"].append(
            {
                "ruleName": "Validate BRQ Requid Id SF validation",
                "result": validation_status,
                "notificationTo": "",
                "msg": "",
            }
        )
    return event
