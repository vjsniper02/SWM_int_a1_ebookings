import json
import logging
import boto3
import os
from botocore.exceptions import ClientError
from functions.a1_2.create_update_integration_job import create_update_integration_job

logger = logging.getLogger("integration_job_update_spots_pre_booking")
logger.setLevel(logging.INFO)

INTEGRATION_NUMBER = "A1"

def get_path_by_param_name(param_name):
        client = boto3.client("ssm")
        response = client.get_parameter(Name=param_name, WithDecryption=True)

        if "Parameter" not in response or response["Parameter"] == None:
            raise Exception(
                f"Parameter does not exist, The parameter '{param_name}' must exist in AWS Parameter Store."
            )

        return response["Parameter"]["Value"]

def update_int_job_spots_loading(event, context):
    logger.info(f"create_update_integration_job started for spots pre booking")
    logger.info(event)
    try:
        chunk_size = int(get_path_by_param_name(os.environ["SPOT_HANDLING_LIMIT"]))
        if "campaignHeaderResponseCode" in event and (
            event["campaignHeaderResponseCode"] == 200
            or event["campaignHeaderResponseCode"] == 201
        ):
            opportunity_id = event["detail"]["sf_payload"]["sf"]["opportunityID"]
            campaign_id = event["detail"]["campaign_code"]
            sf_status = ""
            sf_job_message = ""
            sf_job_type = "Ebooking Spots Loading to Landmark"
            stage = "Ebooking_Spots_Loading_to_Landmark"
            if "spotPrebookingResponseCode" in event:
                operation = "update"
            else:
                operation = "create"
            if "spotPrebookingResponseCode" not in event:
                sf_status = "In Progress"
                sf_job_message = "INF-A1-001: EBooking Spots loading to LMK is in progress."
            if "spotPrebookingStage" in event:
                sf_status = "Failed"
                if event["spotPrebookingResponseCode"] == 503:
                    sf_job_message = "ERR-B3-001 Error Occurred [Landmark Unavailable - Getting 503 response form Landmark]."
                if event["spotPrebookingResponseCode"] == 400:
                    sf_job_message = "ERR-B3-002 Invalid Spot Length"
                if event["spotPrebookingResponseCode"] == 422:
                    sf_job_message = f"ERR-B3-004 Spots previously uploaded for Campaign [{campaign_id}]"
            if (
                "spotPrebookingResponseCode" in event
                and "spotPrebookingStage" not in event
            ):
                sf_status = "Success"
                total_spots = event["total_spots"]
                if "trancheFileCount" in event:
                    if event["trancheFileCount"] > 0:
                        if event["spot_iteration"] == event["trancheFileCount"]:
                            spots_no = event["spot_iteration"] * chunk_size
                            if spots_no > total_spots:
                                remaining = spots_no - total_spots
                                spots_no = spots_no - remaining
                        else:
                            spots_no = event["spot_iteration"] * chunk_size
                        sf_job_message = f"{spots_no} of {total_spots} spots have been processed."
                else:
                    sf_job_message = f"{total_spots} of {total_spots} spots have been processed."
                

            integration_job_response = create_update_integration_job(
                operation, opportunity_id, stage, sf_status, sf_job_message, sf_job_type
            )
            logger.info(f"integration_job_response: {integration_job_response}")
    except Exception as e:
        logger.exception(f"Server error - {str(e)}")
        logger.error(
            f"Error in create/update integration job - spots pre booking stage"
        )
        return event

    # return event to pass to next step in the state machine
    return event
