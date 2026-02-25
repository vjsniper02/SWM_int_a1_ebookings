import json
import logging
import boto3
import os
from botocore.exceptions import ClientError
from functions.a1_2.create_update_integration_job import create_update_integration_job

logger = logging.getLogger("integration_job_update_campaign_header_update")
logger.setLevel(logging.INFO)

INTEGRATION_NUMBER = "A1"


def lambda_handler(event, context):
    logger.info(
        f"create_update_integration_job started for campaign header update stage"
    )
    logger.info(event)
    try:
        opportunity_id = event["detail"]["sf_payload"]["sf"]["opportunityID"]
        campaign_id = event["detail"]["campaign_code"]
        if "campaignHeaderResponseCode" in event:
            operation = "update"
        else:
            operation = "create"
        sf_job_type = "Ebooking Campaign Header Update"
        stage = "Ebooking_Campaign_Header_Update"
        sf_status = "In Progress"
        sf_job_message = "INF-B3-003 The Campaign Header update is in progress."
        if "campaignHeaderStage" in event:
            sf_status = "Failed"
            if event["campaignHeaderResponseCode"] == 503:
                sf_job_message = "ERR-B3-001 Error Occurred [Landmark Unavailable - Getting 503 response form Landmark]."
            if event["campaignHeaderResponseCode"] == 422:
                if event["campaignHeadererror"] == "Campaign is locked.":
                    sf_job_message = f"ERR-B3-003 Campaign [{campaign_id}] Locked."
                elif event["campaignHeadererror"] == "Campaign General: Selected Target Sales Area is not valid for the Deal":
                    sf_job_message = "ERR-B3-018 Target Sales Area is not valid for the Deal."
                else:
                    sf_job_message = f"ERR-B3-005 Fatal Error occurred updating the Campaign Header for Campaign Number [{campaign_id}]"
        if "campaignHeaderResponseCode" in event and "campaignHeaderStage" not in event:
            sf_status = "Success"
            sf_job_message = f"INF-B3-004 The Campaign Header for Campaign [{campaign_id}] was successfully updated."

        integration_job_response = create_update_integration_job(
            operation, opportunity_id, stage, sf_status, sf_job_message, sf_job_type
        )
        logger.info(f"integration_job_response: {integration_job_response}")
    except Exception as e:
        logger.exception(f"Server error - {str(e)}")
        logger.error(
            f"Error in create/update integration job - campaign header update stage"
        )
        raise

    # return event to pass to next step in the state machine
    return event
