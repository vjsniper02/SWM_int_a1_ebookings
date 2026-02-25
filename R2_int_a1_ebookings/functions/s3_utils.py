import boto3

DEFAULT_REGION = "ap-southeast-2"  # Sydney


def save_to_s3(Body, Bucket, Key, Region=None):
    client = boto3.client("s3", region_name=(Region or DEFAULT_REGION))
    response = client.put_object(Body=Body, Bucket=Bucket, Key=Key)
    return response


def read_from_s3(Bucket, Key, Encoding="utf-8", Region=None):
    """
    Read an object content from S3

    :param Bucket str: The bucket name
    :param Key str: The key of the object
    :param Encoding str: Optional and the default value is utf-8. If Encoding is None or "binary", the raw binary data will be returned.
    :param Region str: Optional and the default value is ap-southeast-2 Sydney.
    """
    client = boto3.client("s3", region_name=(Region or DEFAULT_REGION))
    s3_object = client.get_object(Bucket=Bucket, Key=Key)
    content = s3_object["Body"].read()
    if not Encoding or Encoding == "binary":
        return content
    else:
        return content.decode(Encoding)
