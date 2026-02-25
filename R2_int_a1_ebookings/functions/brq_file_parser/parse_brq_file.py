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
CEE_NOTIFICATION_ENGINE = os.environ["CEE_NOTIFICATION_ENGINE"]
step_function = boto3_client("stepfunctions", region_name=AWS_REGION)
lambda_client = boto3_client("lambda", region_name=AWS_REGION)

brq_header_slice_config = {
    "GenerationDate": [0, 8],
    "GenerationTime": [8, 12],
    "NetworkId": [12, 18],
    "NetworkName": [18, 58],
    "AgencyId": [58, 64],
    "AgencyName": [64, 104],
    "BookingDetailRecordCounter": [104, 110],
    "BookingTotalGrossValue": [110, 120],
    "ProposedDetailRecordCounter": [120, 126],
    "ProposedTotalGrossValue": [126, 136],
    "NarrativeRecordCounter": [136, 138],
    "NetworkDomainName": [138, 178],
    "NetworkContactName": [178, 208],
    "NetworkContactEmail": [208, 278],
    "AgencyDomainName": [278, 318],
    "AgencyContactName": [318, 348],
    "AgencyContactEmail": [348, 418],
}

brq_detail_records_slice_config = {
    "WCDate": [258, 266],
    "ClientId": [0, 6],
    "ClientName": [6, 46],
    "ClientProductId": [46, 52],
    "ClientProductName": [52, 92],
    "StationId": [92, 98],
    "StationName": [98, 138],
    "UniqueNetworkProposedSpotId": [138, 158],
    "UniqueNetworkPreviousSpotId": [158, 178],
    "UniqueNetworkParentSpotId": [178, 198],
    "UniqueAgencyProposedSpotId": [198, 218],
    "UniqueAgencyPreviousSpotId": [218, 238],
    "UniqueAgencyParentSpotId": [238, 258],
    "ProposedDay": [266, 273],
    "ProposedStartEndTime": [273, 281],
    "RequestedDay": [281, 288],
    "RequestedTime": [288, 296],
    "ProposedSize": [296, 304, int],
    "ProposedGrossRate": [304, 314],
    "ProposedNetRate": [314, 324],
    "RequestedSize": [324, 332, int],
    "RequestedGrossRate": [
        332,
        342,
    ],
    "RequestedNetRate": [342, 352],
    "ProposedProgram": [352, 392],
    "RequestedProgram": [392, 432],
    "KeyNumber": [432, 452],
    "MaterialInstruction": [452, 512],
    "DemographicOneThousand": [512, 519],
    "DemographicTwoThousand": [519, 526],
    "DemographicThreeThousand": [526, 533],
    "DemographicFourThousand": [533, 540],
    "RecordType": [540, 542],
    "RatingsOverrideFlag1": [542, 543],
    "RatingsOverrideFlag2": [543, 544],
    "RatingsOverrideFlag3": [544, 545],
    "RatingsOverrideFlag4": [545, 546],
    "DemographicCodeOne": [546, 561],
    "DemographicOneTarp": [561, 564],
    "DemographicCodeTwo": [564, 579],
    "DemographicTwoTarp": [579, 582],
    "DemographicCodeThree": [582, 597],
    "DemographicThreeTarp": [597, 600],
    "DemographicCodeFour": [600, 615],
    "DemographicFourTarp": [615, 618],
    "BookingModifiers": [618, None],
}


def santize_brq_line_data(key, line_data):
    if line_data != "":
        if key in [
            "GenerationDate",
            "GenerationTime",
            "BookingDetailRecordCounter",
            "ProposedDetailRecordCounter",
            "NarrativeRecordCounter",
            "ProposedStartEndTime",
            "ProposedSize",
            "ProposedGrossRate",
            "ProposedNetRate",
            "RequestedSize",
        ]:
            return int(line_data)
        if key in [
            "BookingTotalGrossValue",
            "ProposedTotalGrossValue",
            "RequestedGrossRate",
            "RequestedNetRate",
        ]:
            integer = line_data[:8].strip()
            decimal = line_data[-2:].strip()
            return float(f"{integer}.{decimal}")
        if key in [
            "DemographicOneTarp",
            "DemographicTwoTarp",
            "DemographicThreeTarp",
            "DemographicFourTarp",
        ]:
            integer = line_data[:2].strip()
            decimal = line_data[-1:].strip()
            return float(f"{integer}.{decimal}")
        if key in [
            "BookingModifiers",
        ]:
            if line_data:
                str_value = line_data[:-2]  # remove the ending //
                n = 2  # group by 2 characters
                return [str_value[i : i + n] for i in range(0, len(str_value), n)]
            else:
                return []
    return line_data


def prepare_brq_json(context, event_id,brq_file_name_list, brq_type="", ignore_list=[]):
    brq_object_wrapper = {
        "header": {},
        "narrativeRecords": [],
        "details": [],
        "removedDetails": [],
    }
    detail_records = {}
    removed_detail_records = {}
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(os.environ["EBOOKINGS_S3_TEMP_BUCKET"])
    brq_file_name = ""
    booking_request_id = ""
    line_data = ""
    from_email = ""
    pdf_attached = "No"
    current_date_lines = []
    other_date_lines = []
    header_line_data = ""
    has_past_date_records = False
    custom_logger.info("BRQ file parsing started", context, correlationId=event_id)
    current_sunday = get_current_week_sunday()
    current_sunday = datetime.strptime(f"{current_sunday}", "%Y-%m-%d")
    for obj in bucket.objects.filter(Prefix=brq_file_name_list):
        if obj.key not in ignore_list:
            if obj.key.endswith(".brq"):
                brq_file_name = obj.key
                custom_logger.info(f"Working BRQ File:{brq_file_name}", context, correlationId=event_id)
                (from_email, booking_request_id, brq_file_name) = resolve_brq_file_name(
                    brq_file_name, context, event_id, brq_type
                )
                line_count = 0
                for line in obj.get()["Body"].read().decode("utf-8").splitlines():
                    if line_count < 1:
                        header_line_data = line
                        # Initialize lists to store header data
                        current_date_lines = [header_line_data]
                        other_date_lines = [header_line_data]
                        for key in brq_header_slice_config:
                            brq_object_wrapper["header"][key] = santize_brq_line_data(
                                key,
                                line[
                                    brq_header_slice_config[key][
                                        0
                                    ] : brq_header_slice_config[key][1]
                                ].strip(),
                            )
                    else:
                        if line[0:6].strip() != "EOF//":
                            if line[-2:] == "//":
                                for key in brq_detail_records_slice_config:
                                    if key == "WCDate":
                                        wcdate = santize_brq_line_data(
                                            key,
                                            line[
                                                brq_detail_records_slice_config[key][
                                                    0
                                                ] : brq_detail_records_slice_config[
                                                    key
                                                ][
                                                    1
                                                ]
                                            ].strip(),
                                        )
                                        wcdate_converted = sanitize_date_format(wcdate)
                                        wcdate_converted = datetime.strptime(
                                            f"{wcdate_converted}", "%Y-%m-%d"
                                        )
                                        if wcdate_converted >= current_sunday:
                                            has_past_date_records = False
                                            current_date_lines.append(line)
                                        else:
                                            has_past_date_records = True
                                            other_date_lines.append(line)
                                    if brq_detail_records_slice_config[key][1] == None:
                                        if has_past_date_records == True:
                                            removed_detail_records[key] = (
                                                santize_brq_line_data(
                                                    key,
                                                    line[
                                                        brq_detail_records_slice_config[
                                                            key
                                                        ][0] :
                                                    ].strip(),
                                                )
                                            )
                                        else:
                                            detail_records[key] = santize_brq_line_data(
                                                key,
                                                line[
                                                    brq_detail_records_slice_config[
                                                        key
                                                    ][0] :
                                                ].strip(),
                                            )
                                    else:
                                        if has_past_date_records == True:
                                            removed_detail_records[key] = (
                                                santize_brq_line_data(
                                                    key,
                                                    line[
                                                        brq_detail_records_slice_config[
                                                            key
                                                        ][
                                                            0
                                                        ] : brq_detail_records_slice_config[
                                                            key
                                                        ][
                                                            1
                                                        ]
                                                    ].strip(),
                                                )
                                            )
                                        else:
                                            detail_records[key] = santize_brq_line_data(
                                                key,
                                                line[
                                                    brq_detail_records_slice_config[
                                                        key
                                                    ][
                                                        0
                                                    ] : brq_detail_records_slice_config[
                                                        key
                                                    ][
                                                        1
                                                    ]
                                                ].strip(),
                                            )
                                if has_past_date_records == True:
                                    brq_object_wrapper["removedDetails"].append(
                                        removed_detail_records
                                    )
                                else:
                                    brq_object_wrapper["details"].append(detail_records)
                                removed_detail_records = {}
                                detail_records = {}
                            else:
                                line_data = line.strip()
                                brq_object_wrapper["narrativeRecords"].append(line_data)
                                current_date_lines.append(line_data)
                                other_date_lines.append(line_data)
                                line_data = ""
                    line_count += 1
                current_date_lines.append("EOF//")
                other_date_lines.append("EOF//")

            else:
                if obj.key.endswith(".pdf"):
                    pdf_attached = "Yes"
                custom_logger.info(f"PDF File attachment status", context, correlationId=event_id, attached=pdf_attached, fileName=obj.key)

    if len(brq_object_wrapper["details"]) == 0:
        brq_object_wrapper["details"] = brq_object_wrapper["removedDetails"]
    return (
        json.dumps(brq_object_wrapper),
        brq_file_name,
        booking_request_id,
        from_email,
        pdf_attached,
        current_date_lines,
        other_date_lines,
    )


def push_file_to_temp_s3(file_prefix, file_data, bucket_name, extension):
    filename = file_prefix + extension
    s3 = boto3.resource("s3")
    s3.Object(bucket_name, filename).put(Body=file_data)


def rename_file_in_s3(bucket_name, old_key, new_key):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    try:
        copy_source = {"Bucket": bucket_name, "Key": old_key}
        bucket.copy(copy_source, new_key)
        bucket.Object(old_key).delete()
    except Exception as e:
        custom_logger.info(f"Error occured while renaming the file : {e}")


def remove_brq_extension(filename):
    if filename.endswith(".brq"):
        return filename[:-4]
    return filename


def get_current_week_sunday():
    today = date.today()
    days_to_last_sunday = (today.weekday() + 1) % 7
    last_sunday = today - timedelta(days=days_to_last_sunday)
    return last_sunday


def sanitize_date_format(wc_date):
    dateTimeObj = datetime.strptime(wc_date, "%Y%m%d")
    wc_date = dateTimeObj.date()
    wc_date = wc_date.strftime("%Y-%m-%d")
    return wc_date


def update_header_line_data(line, updates):
    # Sort the updates by start index in descending order to avoid messing up indices as we replace
    sorted_updates = sorted(updates.items(), key=lambda x: x[0][0], reverse=True)

    for (start, end), new_value in sorted_updates:
        line = line[:start] + new_value + line[end:]

    return line


def lambda_handler(event, context):
    try:
        event_id = event["id"]
        custom_logger.info(
            f"BRQ file parsing started", context, correlationId=event_id, data=event
        )
        common_utils = CommonUtils(event, custom_logger, context)
        sf_response = {}
        key = event["Records"][0]["s3"]["object"]["key"]
        # Get the list of files from S3 bucket based on the extension list provided on the function argument
        (
            brq_file_name_list,
            child_brq_file_list,
            other_file_list,
        ) = common_utils.get_s3_files_for_extension(
            os.environ["EBOOKINGS_S3_TEMP_BUCKET"], key, [".brq"]
        )
        (from_email, booking_request_id, brq_file_name) = resolve_brq_file_name(
            brq_file_name_list[0], context, event_id, "PARENT"
        )
        event["brqFromEmail"] = from_email
        (
            brq_json_data,
            brq_file_name,
            booking_request_id,
            from_email,
            pdf_attached,
            current_date_lines,
            other_date_lines,
        ) = prepare_brq_json(context, event_id,key + "/")
        custom_logger.info(
            f"BRQ file name:{brq_file_name}",
            context,
            correlationId=event_id,
        )
        custom_logger.info(
            f"BRQ ID:{booking_request_id}",
            context,
            correlationId=event_id,
        )
        custom_logger.info(
            f"From email:{from_email}",
            context,
            correlationId=event_id,
        )
        brq_json_data_converted = json.loads(brq_json_data)
        total_length = 10
        overall_removed_budget = 0
        overall_current_budget = 0
        # Calculate removed Budget and spot count values to update header data
        removed_spots_data = brq_json_data_converted["removedDetails"]
        removed_spots = len(removed_spots_data)

        for index, elem in enumerate(removed_spots_data):
            overall_removed_budget += float(elem["RequestedGrossRate"])
        overall_removed_budget = int(round(overall_removed_budget * 100))
        overall_removed_budget = f"{overall_removed_budget:0{total_length}d}"
        removed_updates = {
            (120, 126): str(removed_spots).zfill(6),
            (126, 136): str(overall_removed_budget),
        }
        updated_header_removed_data = update_header_line_data(
            other_date_lines[0], removed_updates
        )
        other_date_lines[0] = updated_header_removed_data

        # Calculate current Budget and spot count values to update header data
        current_spots_data = brq_json_data_converted["details"]
        current_spots = len(current_spots_data)

        for index, elem in enumerate(current_spots_data):
            overall_current_budget += float(elem["RequestedGrossRate"])
        overall_current_budget = int(round(overall_current_budget * 100))
        overall_current_budget = f"{overall_current_budget:0{total_length}d}"
        current_updates = {
            (120, 126): str(current_spots).zfill(6),
            (126, 136): str(overall_current_budget),
        }
        updated_header_current_data = update_header_line_data(
            current_date_lines[0], current_updates
        )
        current_date_lines[0] = updated_header_current_data

        # Merge/join list elements to prepare brq with header and spots data
        current_brq_data = "\n".join(current_date_lines)
        removed_brq_data = "\n".join(other_date_lines)
        brq_file_name = remove_brq_extension(brq_file_name)
        push_file_to_temp_s3(
            brq_file_name,
            brq_json_data,
            os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
            ".json",
        )
        if removed_spots > 0:
            rename_file_in_s3(
                os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
                key + "/" + from_email + "_" + brq_file_name + ".brq",
                key + "/" + from_email + "_" + brq_file_name + "-Original.brq",
            )
            push_file_to_temp_s3(
                key + "/" + from_email + "_" + brq_file_name,
                current_brq_data,
                os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
                ".brq",
            )
            push_file_to_temp_s3(
                key + "/" + from_email + "_" + brq_file_name + "-Removed",
                removed_brq_data,
                os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
                ".brq",
            )

        custom_logger.info(
            f"Files processed succesfully into '{os.environ['EBOOKINGS_S3_TEMP_BUCKET']}'",
            context,
            correlationId=event_id,
        )

        time.sleep(60)
        return {
            "id":event_id,
            "parseBrqFile": {
                "response": {
                    "status": "success",
                    "key": key,
                    "brq_file_name": brq_file_name,
                    "booking_request_id": booking_request_id,
                    "from_email": from_email,
                    "pdf_attached": pdf_attached,
                    "removed_spots": removed_spots,
                    "removed_amount": overall_removed_budget,
                }
            }
        }
    except UnicodeDecodeError as e:
        custom_logger.error(
            f"Got into UnicodeDecode Error exception",
            context,
            correlationId=event_id,
            error=f"{e}",
        )
        subject = "BRQ is corrupt"
        body = "The BRQ sent through is corrupt. Please refer to attached email and follow up with Agency/Direct Client."
        event = {"brqDirPath": key}
        recipients = [from_email]
        common_utils.send_email_notification(recipients, subject, body, event, [".eml"])
        time.sleep(15)
        custom_logger.info(
            f"'BRQ is corrupt' email sent", context, correlationId=event_id
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
            error=f"{e}",
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
