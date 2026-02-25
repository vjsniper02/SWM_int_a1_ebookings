import json
import boto3
from botocore.exceptions import ClientError
import logging
import os


logger = logging.getLogger("a1_processing statemachine")
logger.setLevel(logging.INFO)


def create_update_integration_job(
    operation, opportunity_id, stage, sf_status, sf_job_message, sf_job_type
):
    client = boto3.client("lambda")

    updateReqPayload = {
        "allOrNone": True,
        "compositeRequest": [
            {
                "method": "PATCH",
                "referenceId": "IntegrationJobUpdate",
                "url": "/services/data/v58.0/sobjects/Integration_Job__c/IntegrationJob_ExId__c/"
                + stage
                + "+"
                + opportunity_id,
                "body": {
                    "Status__c": sf_status,
                    "Job_Message__c": sf_job_message,
                    "Platform__c": "LandMark",
                    "Job_Type__c": sf_job_type,
                    "Opportunity__c": opportunity_id,
                },
            }
        ],
    }

    reqPayload = updateReqPayload

    payload = {"invocationType": "COMPOSITE", "payload": reqPayload}

    try:
        salesforceResponse = client.invoke(
            FunctionName=os.environ["SALESFORCE_ADAPTOR"],
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        logger.info(salesforceResponse)

        salesforcePayload = json.load(salesforceResponse["Payload"])
        logger.info(salesforcePayload)
        return salesforcePayload
    except Exception as e:
        logger.exception(f"Server error - {str(e)}")

    
