import json
import logging
import os
import uuid
import zipfile
import io
from io import BytesIO
from boto3 import client, resource
import boto3
import botocore
import uuid
from swm_logger.swm_common_logger import LambdaLogger

custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])
UnqID = uuid.uuid4()
AWS_REGION = os.environ["SEIL_AWS_REGION"]

INTEGRATION_NUMBER = os.environ["INTEGRATION_NUMBER"]


def extract_brq_file(filekey, source_bucket_name, temp_bucket, event_id, context):
    try:
        zipped_file = s3_resource.Object(bucket_name=source_bucket_name, key=filekey)
        buffer = BytesIO(zipped_file.get()["Body"].read())
        zipped = zipfile.ZipFile(buffer)
        for file in zipped.namelist():
            custom_logger.info(
                f"File found in ZIP archive",
                context,
                correlationId=event_id,
                integrationId=INTEGRATION_NUMBER,
                fileName=file,
            )
            final_file_path = zipped_file.key + "/" + file

            with zipped.open(file, "r") as f_in:
                unzipped_content = f_in.read()
                temp_bucket.upload_fileobj(
                    io.BytesIO(unzipped_content),
                    final_file_path,
                    ExtraArgs={"ContentType": "text/plain"},
                )
    except Exception as e:
        custom_logger.error(
            f"Error: Unable to gzip & upload file",
            context,
            correlationId=event_id,
            integrationId=INTEGRATION_NUMBER,
            error=f"{e}",
        )


def lambda_handler(event, context):
    lambda_client = boto3.client("lambda")
    event["id"] = str(uuid.uuid4())
    event_id = event["id"]
    custom_logger.info(
        "Event data from S3/E-Trans",
        context,
        correlationId=event_id,
        integrationId=INTEGRATION_NUMBER,
        data=event,
    )
    global s3_resource
    s3_resource = resource("s3")
    source_bucket_name = os.environ["EBOOKINGS_S3_FILEIN_BUCKET"]
    temp_bucket = s3_resource.Bucket(os.environ["EBOOKINGS_S3_TEMP_BUCKET"])
    key = event["Records"][0]["s3"]["object"]["key"]
    extract_brq_file(key, source_bucket_name, temp_bucket, event_id, context)

    try:
        custom_logger.info(
            f"Invoking LHS service to push message to Queue",
            context,
            correlationId=event_id,
            integrationId=INTEGRATION_NUMBER,
        )

        processing_state_machine_arn = os.environ["EBOOKING_PARSE_BRQ_ENGINE_ARN"]

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

        custom_logger.info(
            publishQueueResponse,
            context,
            correlationId=event_id,
            integrationId=INTEGRATION_NUMBER,
        )

    except Exception as e:
        custom_logger.error(
            f"Error invoking LHS service",
            context,
            correlationId=event_id,
            integrationId=INTEGRATION_NUMBER,
            error=f"{str(e)}",
        )
        raise
