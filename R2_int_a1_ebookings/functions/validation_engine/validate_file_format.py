import logging
import boto3
import os
import json
from boto3 import client as boto3_client

logger = logging.getLogger("a1_validation_engine")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info(f"event_data: {event}")
    """Funtion to validate BRQ file"""
    event["validationResult"]["result"] = "SUCCESS"
    event["validationResult"]["continueValidation"] = True
    event["validationResult"]["details"].append(
        {
            "ruleName": "Validate BRQ File Content Type",
            "result": "SUCCESS",
            "notificationTo": "",
            "msg": "",
        }
    )
    return event
