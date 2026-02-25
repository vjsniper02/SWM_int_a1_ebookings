import boto3
import os


def lambda_handler(event, context):
    """
    Lambda to check the number of files in a tranche folder in S3.
    Expects:
      event = {
        "bucket": "your-bucket-name",
        "tranche_folder": "tranche/"
      }
    Returns:
      {
        "fileCount": <number_of_files>
      }
    """
    correlation_id = event["id"]
    s3 = boto3.client("s3")

    # Get bucket and folder from event or environment
    bucket_name = os.environ["EBOOKINGS_S3_TEMP_BUCKET"]
    
    tranche_folder = event.get("tranche_folder", "tranche/")

    if not bucket_name:
        raise ValueError("Bucket name is required")

    # Initialize file count
    file_count = 0

    # Use pagination for large folders
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=correlation_id +"/"+tranche_folder)

    for page in pages:
        if "Contents" in page:
            file_count += len(page["Contents"])

    event.update({"trancheFileCount": file_count})

    return event
