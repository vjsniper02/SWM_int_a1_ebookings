import logging
import json
import csv
import os
import urllib.parse
import datetime as dt
from datetime import date
import boto3
from datetime import datetime, timedelta
from boto3 import client as boto3_client
from collections import Counter
import time

from swm_logger.swm_common_logger import LambdaLogger
from functions.brq_file_parser.resolve_brq_file_name import resolve_brq_file_name
from functions.common_utils import CommonUtils

custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])

AWS_REGION = os.environ["SEIL_AWS_REGION"]
ARN_SF_ADAPTOR = os.environ["SALESFORCE_ADAPTOR"]
ARN_VALIDATION_ENGINE = os.environ["ARN_VALIDATION_ENGINE_SERVICE"]
CEE_NOTIFICATION_ENGINE = os.environ["CEE_NOTIFICATION_ENGINE"]
step_function = boto3_client("stepfunctions", region_name=AWS_REGION)
lambda_client = boto3_client("lambda", region_name=AWS_REGION)
step_function = boto3_client("stepfunctions", region_name=AWS_REGION)


def get_csv_data(file_name):
    """
    Function to read the csv data from config bucket
    """
    seil_config_bucket_name = os.environ["SEIL_CONFIG_BUCKET_NAME"]
    try:
        s3 = boto3.client("s3")
        s3_object = s3.get_object(Bucket=seil_config_bucket_name, Key=file_name)
        data = s3_object["Body"].read().decode("utf-8-sig").splitlines()
        csv_records = csv.DictReader(data)
        csv_data = list(csv_records)
        return csv_data
    except Exception as e:
        raise RuntimeError(
            f"Error retrieving CSV data from file: {file_name}: {e}"
        ) from e


def get_geo_data(station_code, csv_data):
    """
    Function to retrive geography information from CSV
    """
    try:
        geo = ""
        count = 0
        res_data = None
        for sub in csv_data:
            if sub["BCC"] == station_code:
                res_data = sub
                count += 1
        if count != 0:
            geo = res_data.get("Geography")
        return geo
    except Exception as e:
        custom_logger.error(f"Error returning geography details: {e}")


def get_current_week_sunday():
    today = datetime.now().date()
    sunday = today - timedelta(days=today.weekday() + 1)
    return sunday


def validate_sales_area_codes(station_code_list, station_list, context, event_id):
    # Initialize a list to store codes that are NOT found in the CSV
    failed_codes = []

    try:
        csv_data = get_csv_data(os.environ["SEIL_SALES_AREA_MAPPING_FILE"])
        valid_bcc_codes = {row["BCC"] for row in csv_data}

        # Iterate through each station code provided in the input list
        for code in station_code_list:
            # Check if the current code is present in our set of valid BCC codes
            if code not in valid_bcc_codes:
                for station_item in station_list:
                    if code in station_item["station_id"]:
                        failed_codes.append(
                            code + "  " + station_item["station_name"] + "\n"
                        )

        # After checking all codes, if any failed, log data accordingly
        if failed_codes:
            custom_logger.info(
                f"The following station codes are not found in the sales area mapping: {failed_codes}",
                context,
                correlationId=event_id,
            )
        else:
            custom_logger.info(
                "All station codes in the list are valid.",
                context,
                correlationId=event_id,
            )
        return failed_codes

    except KeyError:
        custom_logger.info(
            f"Error: Environment variable 'SALES_AREA_MAPPING_FILE' not set.",
            context,
            correlationId=event_id,
        )
    except FileNotFoundError:
        custom_logger.info(
            f"Error: Sales area mapping file not found at the specified location.",
            context,
            correlationId=event_id,
        )
    except Exception as e:
        custom_logger.info(
            f"An unexpected error occurred during validation: {e}",
            context,
            correlationId=event_id,
        )


def get_detail_records_summary(detail_records, context, event_id):
    record_summary = {}
    wc_dates = []
    req_spot_size = []
    demo_num = []
    overall_demo_num = []
    geo_list = []
    station_id_list = []
    station_list = []
    geo_data = ""
    overall_budget = 0
    overall_spot_count = 0
    csv_data = get_csv_data(os.environ["SEIL_SALES_AREA_MAPPING_FILE"])
    for index, elem in enumerate(detail_records):
        dateTimeObj = datetime.strptime(elem["WCDate"], "%Y%m%d")
        wc_dates.append(dateTimeObj.date())
        overall_budget += float(elem["RequestedGrossRate"])
        req_spot_size.append(int(elem["RequestedSize"]))
        demo_num.append(str(elem["DemographicCodeOne"]))
        overall_demo_num.extend(
            [
                str(elem["DemographicCodeOne"]),
                str(elem["DemographicCodeTwo"]),
                str(elem["DemographicCodeThree"]),
                str(elem["DemographicCodeFour"]),
            ]
        )
        geo = get_geo_data(elem["StationId"], csv_data)
        if elem["StationId"] != "" and elem["StationId"] not in station_id_list:
            station_id_list.append(elem["StationId"])
            station_list.append(
                {"station_id": elem["StationId"], "station_name": elem["StationName"]}
            )
        if geo != "" and geo not in geo_list:
            geo_list.append(geo)
        overall_spot_count += 1
    if len(geo_list) > 1:
        geo_data = "National"
    else:
        geo_data = geo_list[0]
    record_summary["geo_data"] = geo_data
    current_sunday = get_current_week_sunday()
    record_summary["minWCDate"] = min(wc_dates).strftime("%Y-%m-%d")
    min_date_to_compare = datetime.strptime(
        record_summary["minWCDate"], "%Y-%m-%d"
    ).date()
    if min_date_to_compare < current_sunday:
        record_summary["minWCDate"] = current_sunday.strftime("%Y-%m-%d")
    record_summary["closeDate"] = (date.today() + dt.timedelta(days=2)).strftime(
        "%Y-%m-%d"
    )
    record_summary["maxWCDate"] = (max(wc_dates) + dt.timedelta(days=6)).strftime(
        "%Y-%m-%d"
    )
    record_summary["overallBudget"] = overall_budget
    record_summary["overallSpotCount"] = overall_spot_count
    unique_req_spot_size = list(set(req_spot_size))
    max_spot_length = max(unique_req_spot_size, key=lambda x: x)
    filtered_overall_demo_num = list(filter(None, overall_demo_num))
    demo_num_count = len(list(set(filtered_overall_demo_num)))
    record_summary["spot_length"] = max_spot_length
    record_summary["spot_length_num"] = len(unique_req_spot_size)
    record_summary["demo_count"] = demo_num_count
    record_summary["demo_code"] = demo_num
    record_summary["station_id"] = station_id_list
    record_summary["station_list"] = station_list
    custom_logger.info(
        f"Record summary for spots",
        context,
        correlationId=event_id,
        data=record_summary,
    )
    return record_summary


def get_client_type(client_id, agency_id, context, event_id):
    client_type = ""
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Name,+Id,+Type+from+Account+WHERE+SWM_External_Account_ID__c+='{client_id}'",
    }
    try:
        custom_logger.info(
            f"Connecting salesforce to get the client type",
            context,
            correlationId=event_id,
        )
        invoke_response = lambda_client.invoke(
            FunctionName=ARN_SF_ADAPTOR,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        downstream_response = json.loads(invoke_response["Payload"].read())
        type = downstream_response["records"][0]["Type"]
        if type == "Direct Client":
            client_type = "Direct Client"
        if type == "Agency Client & Direct Client":
            if client_id == agency_id:
                client_type = "Direct Client"
            else:
                client_type = "Agency Client"
        if type == "Agency Client":
            client_type = "Agency Client"
        custom_logger.info(
            f"Response from salesforce",
            context,
            correlationId=event_id,
            data=downstream_response,
        )
        return client_type
    except Exception as e:
        custom_logger.error(
            f"Error getting salesforce client Type:",
            context,
            correlationId=event_id,
            error={e},
        )
        return False


def get_brq_zip_details(file_prefix, bucket):
    details = {"files": []}
    """Funtion to get the files from archived s3 folder"""
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)
    for object_summary in bucket.objects.filter(Prefix=f"{file_prefix}/"):
        s3_path = f"s3://{bucket.name}/{object_summary.key}"
        details["files"].append({"path": s3_path})
        return details


def get_brq_validation_response(
    file_prefix,
    brq_file_name,
    brq_id,
    sf_account_type,
    record_summary,
    ebookings_temp_info,
    from_email,
    context,
    event_id,
    errored_sales_area_list,
):
    event_data = {
        "validationMessages": [],
        "caseContent": [],
        "brqEmail": from_email,
        "brqWcStartDate": record_summary["minWCDate"],
        "brqWcEndDate": record_summary["maxWCDate"],
        "brqZipDetails": ebookings_temp_info,
        "brqJsonPath": os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
        "brqDirPath": file_prefix,
        "errored_sales_area_list": errored_sales_area_list,
        "brqFileName": brq_file_name,
        "brqId": brq_id,
        "oppStage": "",
        "oppData": {},
        "oppId": "",
        "sfAccountType": sf_account_type,
        "lmkProductResponse": {},
        "correlationID": "32rh32fh239fdj2390jd20j20",  # ToDo - fetch the actual correlationId from the context.
        "otherFiles": [],
        "validationResult": {
            "result": "",
            "createOrUpdate": "",
            "continueValidation": True,
            "details": [],
        },
    }
    try:
        invoke_response = step_function.start_execution(
            stateMachineArn=ARN_VALIDATION_ENGINE,
            input=json.dumps(event_data),
        )
    except Exception as e:
        # https://aws.amazon.com/premiumsupport/knowledge-center/lambda-troubleshoot-invoke-error-502-500/
        # Lambda function is not ready. Retry to see if it works.
        if "CodeArtifactUserPendingException" in str(e):
            custom_logger.exception(
                f"Caught CodeArtifactUserPendingException. This is Attempt.",
                context,
                correlationId=event_id,
            )
        else:
            raise  # rethrow the exception if it's not CodeArtifactUserPendingException
    custom_logger.info(
        f"Validation engine invoke response",
        context,
        correlationId=event_id,
        data=invoke_response,
    )
    max_retries = 15
    retry_interval = 60
    for attempt in range(max_retries):
        try:
            describe_response = step_function.describe_execution(
                executionArn=invoke_response["executionArn"]
            )
            describe_response = describe_response
            if "output" not in describe_response:
                raise ValueError("No OUTPUT from Statemachine")
            return describe_response
        except Exception as e:
            custom_logger.info(
                f"Attempt - {attempt}, No OUTPUT from Statemachine, Still processing...",
                context,
                correlationId=event_id,
            )
            if attempt < max_retries - 1:
                custom_logger.info(
                    f"Retrying in {retry_interval} seconds...",
                    context,
                    correlationId=event_id,
                )
                time.sleep(retry_interval)
            else:
                custom_logger.info(
                    "Maximum retry attempts reached. Exiting.",
                    context,
                    correlationId=event_id,
                )
                raise


def read_file_from_s3(bucket_name, file_key):
    session = boto3.Session()
    s3_client = session.client("s3")
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        json_data = json.loads(content)
        return json_data
    except Exception as e:
        return None


def lambda_handler(event, context):
    event_id = event["id"]
    custom_logger.info(
        f"BRQ validation started", context, correlationId=event_id, data=event
    )
    common_utils = CommonUtils(event, custom_logger, context)
    try:
        parse_brq_file_data = event["parseBrqFile"]["response"]
        if parse_brq_file_data["status"] == "success":
            brq_file_name = parse_brq_file_data["brq_file_name"]
            booking_request_id = parse_brq_file_data["booking_request_id"]
            from_email = parse_brq_file_data["from_email"]
            key = parse_brq_file_data["key"]
            brq_json_data = read_file_from_s3(
                os.environ["EBOOKINGS_S3_TEMP_BUCKET"], brq_file_name + ".json"
            )
            record_summary = get_detail_records_summary(
                brq_json_data["details"], context, event_id
            )
            errored_sales_area_list = validate_sales_area_codes(
                record_summary["station_id"],
                record_summary["station_list"],
                context,
                event_id,
            )
            sf_account_type = get_client_type(
                brq_json_data["details"][0]["ClientId"],
                brq_json_data["header"]["AgencyId"],
                context,
                event_id,
            )
            ebookings_temp_info = get_brq_zip_details(
                key, os.environ["EBOOKINGS_S3_TEMP_BUCKET"]
            )
            validation_response = get_brq_validation_response(
                key,
                brq_file_name,
                booking_request_id,
                sf_account_type,
                record_summary,
                ebookings_temp_info,
                from_email,
                context,
                event_id,
                errored_sales_area_list,
            )
            validation_response = json.loads(validation_response["output"])
            custom_logger.info(
                f"Validation engine final response",
                context,
                correlationId=event_id,
                data=validation_response,
            )
            event["record_summary"] = record_summary
            event["validation_response"] = validation_response
            return event
    except UnicodeDecodeError as e:
        custom_logger.error(
            f"Got into UnicodeDecode Error exception",
            context,
            correlationId=event_id,
            error={e},
        )
        common_utils.move_file_s3_to_s3(
            os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
            os.environ["EBOOKINGS_S3_ERROR_BUCKET"],
            os.environ["EBOOKINGS_S3_FILEIN_BUCKET"],
            key,
            brq_file_name,
        )
        custom_logger.info(
            f"Files moved to {os.environ['EBOOKINGS_S3_ERROR_BUCKET']}",
            context,
            correlationId=event_id,
        )
    except (RuntimeError, KeyError, ValueError) as e:
        custom_logger.error(
            f"Got into Runtime/Key Error exception",
            context,
            correlationId=event_id,
            error={e},
        )
        common_utils.move_file_s3_to_s3(
            os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
            os.environ["EBOOKINGS_S3_ERROR_BUCKET"],
            os.environ["EBOOKINGS_S3_FILEIN_BUCKET"],
            key,
            brq_file_name,
        )
        custom_logger.info(
            f"Files moved to {os.environ['EBOOKINGS_S3_ERROR_BUCKET']}",
            context,
            correlationId=event_id,
        )
