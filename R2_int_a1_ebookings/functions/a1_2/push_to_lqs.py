# Refer to from https://dev.azure.com/SevenWestMediaLimited/Project%20Phoenix/_git/R2_int_b3_lmk_camp_header?path=%2Ffunctions%2Fpush_campaign_lqs%2Fb3_push_campaign_lqs.py

import json
import logging
import boto3
import os
from botocore.exceptions import ClientError

logger = logging.getLogger("a1_2_push_payloads_to_lqs_function")
logger.setLevel(logging.INFO)

INTEGRATION_NUMBER = "A1"


def lambda_handler(event, context):
    logger.info(f"Push Queue")
    logger.info(event)
    lambda_client = boto3.client("lambda")

    event_id = event["id"]

    try:
        logger.info(f"Invoke LHS service to push message to Queue")

        processing_state_machine_arn = os.environ[
            "EBOOKING_SPOT_PROCESSING_STATEMACHINE"
        ]

        input_msg = {
            "integrationId": INTEGRATION_NUMBER,
            "groupId": INTEGRATION_NUMBER + str(event_id),
            "deduplicationId": INTEGRATION_NUMBER + str(event_id),
            "correlationId": INTEGRATION_NUMBER + str(event_id),
            "processingStateMachineARN": processing_state_machine_arn,
            "payload": event,
        }

        # Push message to LHS service
        publishQueueResponse = lambda_client.invoke(
            FunctionName=os.environ["LQS_PUBLISHER_FUNCTION"],
            InvocationType="RequestResponse",
            Payload=json.dumps(input_msg),
        )

        logger.info(publishQueueResponse)
        logger.info("End of Flow")

    except Exception as e:
        logger.exception(f"Server error - {str(e)}")
        logger.error(f"Error in A1 LQS Publisher")
        raise

    # return event to pass to next step in the state machine
    return event
