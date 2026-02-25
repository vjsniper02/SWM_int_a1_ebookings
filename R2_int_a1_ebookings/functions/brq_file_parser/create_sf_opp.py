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
    "WCDate": [258, 266],
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
            "RequestedTime",
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


def receive_brq_time(key):
    """Funtion to get the date when the BRQ file was received from E Trans"""
    s3 = boto3.resource("s3")
    s3_object = s3.Object(os.environ["EBOOKINGS_S3_FILEIN_BUCKET"], key)
    receive_datetime = s3_object.last_modified
    receive_date = receive_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    return receive_date


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


def construct_sales_area_details(csv_data, parent_sales_area_no):
    """
    Function to contruct sales area details based on parent sales area number
    """
    try:
        sales_area_details_obj = []
        count = 0
        for sub in csv_data:
            if sub["parentSalesAreanumber"] == parent_sales_area_no:
                res_data = sub
                if count == 0:
                    sales_area_details_obj.append(
                        {
                            "salesAreaNumber": res_data.get("salesAreaNumber"),
                            "isExcluded": False,
                            "percentageSplit": 100,
                            "spotsPercentage": 100,
                        }
                    )
                else:
                    sales_area_details_obj.append(
                        {
                            "salesAreaNumber": res_data.get("salesAreaNumber"),
                            "isExcluded": False,
                            "percentageSplit": 0,
                        }
                    )
                count += 1
        custom_logger.info(f"Mapped Sales area details: {sales_area_details_obj}")
        return sales_area_details_obj
    except Exception as e:
        custom_logger.info(f"Error mapping sales area details: {e}")


def get_demo_details(demo_code_one):
    """Function to retrive demographic trading information from CSV"""
    try:
        message = []
        demo_mapping = ""
        comparing_attribute = ""
        count = 0
        csv_data = get_csv_data(os.environ["SEIL_DEMO_MAPPING_FILE"])
        custom_logger.info(demo_code_one)
        demo_code_one = list(demo_code_one)
        most_frequent_demo = max(set(demo_code_one), key=demo_code_one.count)
        custom_logger.info(f"most_frequent_demo: {most_frequent_demo}")
        demo_code_one = most_frequent_demo
        if demo_code_one and demo_code_one.isdigit():
            comparing_attribute = "Numeric Identifier"
        else:
            comparing_attribute = "SMD Code"
        for sub in csv_data:
            if sub[comparing_attribute] == demo_code_one:
                res_data = sub
                count += 1
        if count < 1:
            message.append(f"BRQ Demo [{demo_code_one}] is not a SWM Trading Demo")
            custom_logger.info(f"No demographic details found for '{demo_code_one}'")
            demo_name = ""
            demo_number = ""
        else:
            demo_name = res_data.get("Trading Demo")
            demo_number = res_data.get("Landmark Code")
            smd_code = res_data.get("SMD Code")
            custom_logger.info(
                f"Demographic details found: {demo_name}, {demo_number}, {smd_code}"
            )
            if demo_code_one and demo_code_one[0].isdigit():
                if smd_code.strip() == "":
                    message.append(
                        f"BRQ Demo [{demo_code_one}] is not a SWM Trading Demo"
                    )
                else:
                    message.append(
                        f"‘BRQ Demo [{demo_code_one}] has been found to be SWM Trading Demo [{smd_code}]"
                    )
        return demo_name, demo_number, message
    except Exception as e:
        custom_logger.info(f"Error returning demographic details: {e}")


def get_client_type(client_id, agency_id):
    client_type = ""
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Name,+Id,+Type+from+Account+WHERE+SWM_External_Account_ID__c+='{client_id}'",
    }
    try:
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
        custom_logger.info(downstream_response)
        return client_type
    except Exception as e:
        custom_logger.info(f"Error getting salesforce Record Type Id: {e}")
        return False


def get_sf_account_id(agency_id):
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Id,+Name+from+Account+WHERE+SWM_External_Account_ID__c+='{agency_id}'+OR+SWM_Additional_External_Ids__c+='{agency_id}'+OR+SWM_Additional_External_Ids__c+LIKE+'{agency_id};%'+OR+SWM_Additional_External_Ids__c+LIKE+'%;{agency_id};%'+OR+SWM_Additional_External_Ids__c+LIKE+'%;{agency_id}'",
    }
    try:
        invoke_response = lambda_client.invoke(
            FunctionName=ARN_SF_ADAPTOR,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        downstream_response = json.loads(invoke_response["Payload"].read())
        downstream_response = downstream_response["records"][0]["Id"]
        custom_logger.info(downstream_response)
        return downstream_response
    except Exception as e:
        custom_logger.info(f"Error getting salesforce Account Id: {e}")
        return False


def get_brq_campaign_type(client_type):
    if client_type == "Direct Client":
        return "Direct to Advertiser"
    return "Agency Buy"


def get_ssm_parameter(parameter_name: str) -> str:
    """Function to retrive AWS SSM parameters"""
    try:
        ssm = boto3.client("ssm")
        response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        return response["Parameter"]["Value"]
    except Exception as e:
        raise RuntimeError(f"Error retrieving parameter '{parameter_name}': {e}") from e


def get_sf_recordtype_id(opp_relationship):
    if opp_relationship == "PARENT":
        recordtype_name = get_ssm_parameter(os.environ["SF_PARENT_RECORD_TYPE_NAME"])
    else:
        recordtype_name = get_ssm_parameter(os.environ["SF_RECORD_TYPE_NAME"])
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Id+from+RecordType+WHERE+DeveloperName+='{recordtype_name}'+AND+SobjectType+='Opportunity'+LIMIT+1",
    }
    try:
        invoke_response = lambda_client.invoke(
            FunctionName=ARN_SF_ADAPTOR,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        downstream_response = json.loads(invoke_response["Payload"].read())
        downstream_response = downstream_response["records"][0]["Id"]
        custom_logger.info(downstream_response)
        return downstream_response
    except Exception as e:
        custom_logger.info(f"Error getting salesforce Record Type Id: {e}")
        return False


def get_sf_buying_agency_id(validation_response):
    lmk_product_response = validation_response["lmkProductResponse"]
    lmk_buyer_id = lmk_product_response.get("agencyCode", "")
    if lmk_buyer_id != "":
        payload = {
            "invocationType": "QUERY",
            "query": f"SELECT+Id,+Name+from+Account+WHERE+SWM_LandMark_ID__c+='{lmk_buyer_id}'",
        }
        try:
            invoke_response = lambda_client.invoke(
                FunctionName=ARN_SF_ADAPTOR,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            downstream_response = json.loads(invoke_response["Payload"].read())
            downstream_response = downstream_response["records"][0]["Id"]
            custom_logger.info(downstream_response)
            return downstream_response
        except Exception as e:
            custom_logger.info(f"Error getting salesforce account Id: {e}")
            return False
    else:
        return ""


def prepare_sf_opp_payload(
    event,
    brq_json_data,
    brq_file_name,
    booking_request_id,
    pdf_attached,
    receive_time,
    record_summary,
    validation_response,
    opp_relationship="",
    parent_opp_id="",
):
    messages = []
    split_status = False
    removed_spot_count = 0
    lmk_product_response = validation_response["lmkProductResponse"]
    lmk_product_id = lmk_product_response.get("productCode", "")
    lmk_product_name = lmk_product_response.get("productName", "")
    billing_agency_id = get_sf_account_id(brq_json_data["header"]["AgencyId"])
    buying_agency_id = get_sf_buying_agency_id(validation_response)
    if buying_agency_id == "":
        buying_agency_id = billing_agency_id
    demo_name, demo_number, demo_message = get_demo_details(record_summary["demo_code"])

    if opp_relationship != "CHILD":
        removed_spot_count = event["parseBrqFile"]["response"]["removed_spots"]
        removed_spot_amount = event["parseBrqFile"]["response"]["removed_amount"]
        value_spot_amount = int(removed_spot_amount) // 100

        if removed_spot_count > 1:
            messages.append(
                f"Note: {removed_spot_count} spots, with a value of ${value_spot_amount:,.2f}, have been removed from this Opportunity, due to the spots being before the commencing date"
            )

    if (
        brq_json_data["header"]["ProposedDetailRecordCounter"]
        != record_summary["overallSpotCount"]
    ) and (removed_spot_count == 0):
        messages.append(
            "The Number of Detailed Spot lines, does not match the Number of Spots in the Header line"
        )
    if (
        brq_json_data["header"]["ProposedTotalGrossValue"]
        != record_summary["overallBudget"]
    ) and (removed_spot_count == 0):
        messages.append(
            "The total Spot Gross Value amount does not match the Gross Value amount in the Header line"
        )

    all_err_msgs = [
        *messages,
        *demo_message,
        *validation_response["validationMessages"],
    ]
    swm_err_msg = "\n\n".join(all_err_msgs)
    custom_logger.info(f"SWM Error messages: {swm_err_msg}")
    if opp_relationship != "":
        split_status = True
    if (
        get_client_type(
            brq_json_data["details"][0]["ClientId"], brq_json_data["header"]["AgencyId"]
        )
        == "Direct Client"
    ):
        eventData = {
            "entity": "Opportunity",
            "invocationType": "SOBJECTS",
            "brqid": booking_request_id,
            "payload": {
                "BRQ_Received_DateTime__c": receive_time,
                "AccountId": get_sf_account_id(brq_json_data["details"][0]["ClientId"]),
                "SWM_Billing_Agency__c": get_sf_account_id(
                    brq_json_data["details"][0]["ClientId"]
                ),
                "SWM_Buying_Agency__c": get_sf_account_id(
                    brq_json_data["details"][0]["ClientId"]
                ),
                "SWM_Campaign_Type__c": get_brq_campaign_type(
                    get_client_type(
                        brq_json_data["details"][0]["ClientId"],
                        brq_json_data["header"]["AgencyId"],
                    )
                ),
                "Name": "TestTest Op 000123",
                "SWM_Overall_Budget__c": record_summary["overallBudget"],
                "SWM_Start_Date__c": record_summary["minWCDate"],
                "SWM_End_Date__c": record_summary["maxWCDate"],
                "SWM_Demographic_Number__c": demo_number,
                "SWM_Demographic_Name__c": demo_name,
                "StageName": "In Progress",
                "CloseDate": record_summary["closeDate"],
                "SWM_Landmark_Product_ID__c": lmk_product_id,
                "SWM_Landmark_Product_Name__c": lmk_product_name,
                "SWM_Number_of_Spots__c": record_summary["overallSpotCount"],
                # "SWM_Spots_created__c": "No",
                "SWM_Duration__c": record_summary["spot_length"],
                "SWM_No_of_BRQ_Spot_Lengths__c": record_summary["spot_length_num"],
                "SWM_No_of_BRQ_Demographics__c": record_summary["demo_count"],
                "SWM_Geography__c": record_summary["geo_data"],
                "SWM_BRQ_Request_ID__c": booking_request_id,
                "BRQ_FileName__c": brq_file_name,
                "SWM_E_Booking_PDF_attached__c": pdf_attached,
                "RecordTypeId": get_sf_recordtype_id(opp_relationship),
                "SWM_Error_Message__c": swm_err_msg,
                "SWM_Split_Opportunity__c": split_status,
            },
        }
        if opp_relationship == "PARENT":
            eventData["payload"]["StageName"] = "Initiated"
        if opp_relationship == "CHILD":
            eventData["payload"]["SWM_Parent_Opportunity__c"] = parent_opp_id
    else:
        eventData = {
            "entity": "Opportunity",
            "invocationType": "SOBJECTS",
            "brqid": booking_request_id,
            "payload": {
                "BRQ_Received_DateTime__c": receive_time,
                "AccountId": get_sf_account_id(brq_json_data["details"][0]["ClientId"]),
                "SWM_Campaign_Type__c": get_brq_campaign_type(
                    get_client_type(
                        brq_json_data["details"][0]["ClientId"],
                        brq_json_data["header"]["AgencyId"],
                    )
                ),
                "Name": "TestTest Op 000123",
                "SWM_Billing_Agency__c": billing_agency_id,
                "SWM_Buying_Agency__c": buying_agency_id,
                "SWM_Overall_Budget__c": record_summary["overallBudget"],
                "SWM_Start_Date__c": record_summary["minWCDate"],
                "SWM_End_Date__c": record_summary["maxWCDate"],
                "SWM_Demographic_Number__c": demo_number,
                "SWM_Demographic_Name__c": demo_name,
                "StageName": "In Progress",
                "CloseDate": record_summary["closeDate"],
                "SWM_Landmark_Product_ID__c": lmk_product_id,
                "SWM_Landmark_Product_Name__c": lmk_product_name,
                "SWM_Number_of_Spots__c": record_summary["overallSpotCount"],
                # "SWM_Spots_created__c": "No",
                "SWM_Duration__c": record_summary["spot_length"],
                "SWM_No_of_BRQ_Spot_Lengths__c": record_summary["spot_length_num"],
                "SWM_No_of_BRQ_Demographics__c": record_summary["demo_count"],
                "SWM_Geography__c": record_summary["geo_data"],
                "SWM_BRQ_Request_ID__c": booking_request_id,
                "BRQ_FileName__c": brq_file_name,
                "SWM_E_Booking_PDF_attached__c": pdf_attached,
                "RecordTypeId": get_sf_recordtype_id(opp_relationship),
                "SWM_Error_Message__c": swm_err_msg,
                "SWM_Split_Opportunity__c": split_status,
            },
        }
        if opp_relationship == "PARENT":
            eventData["payload"]["StageName"] = "Initiated"
        if opp_relationship == "CHILD":
            eventData["payload"]["SWM_Parent_Opportunity__c"] = parent_opp_id
    custom_logger.info(f"sf request paylod: {eventData}")
    return eventData


def create_sf_opportunity(payload):
    payload["method"] = "POST"
    payload["oppId"] = ""
    try:
        invoke_response = lambda_client.invoke(
            FunctionName=ARN_SF_ADAPTOR,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        custom_logger.info(f" Opp create response: {invoke_response}")
        return json.loads(invoke_response["Payload"].read())
    except Exception as e:
        custom_logger.info(f" Error occured in Opp Creation: {e}")
        return None


def push_file_via_s3_link_api(opportunity_id, s3_obj):
    payload = {
        "record_id": opportunity_id,
        "invocationType": "S3LINKAPI",
        "s3_obj": s3_obj,
    }
    custom_logger.info(f"Pushing Payload: {payload}")
    try:
        invoke_response = lambda_client.invoke(
            FunctionName=ARN_SF_ADAPTOR,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        downstream_response = json.loads(invoke_response["Payload"].read())
        custom_logger.info(downstream_response)
        custom_logger.info(f"Response for file to Opp: {invoke_response}")
        return downstream_response
    except Exception as e:
        custom_logger.info(f"Error uploading file to API: {e}")
        return False


def del_file_from_source_bucket(source_bucket, key):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        # Delete the file from the source bucket
        s3.delete_object(Bucket=source_bucket, Key=key)
    except Exception as e:
        custom_logger.info(f"Error deleting file: {e}")


def initiate_post_opp_creation_steps(opp_id, key, ignore_list=[]):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(os.environ["EBOOKINGS_S3_TEMP_BUCKET"])
    # ignore = s3.Bucket(os.environ["EBOOKINGS_S3_TEMP_BUCKET"]).objects.filter(Prefix=key + "/split/")
    for obj in bucket.objects.filter(Prefix=key + "/"):
        if obj.key not in ignore_list:
            s3_obj = {"bucket_name": "", "key": ""}
            s3_obj["bucket_name"] = obj.bucket_name
            s3_obj["key"] = obj.key
            push_file_via_s3_link_api(opp_id, json.dumps(s3_obj))
    del_file_from_source_bucket(os.environ["EBOOKINGS_S3_FILEIN_BUCKET"], key)


def prepare_brq_json(
    context, event_id, brq_file_name_list, brq_type="", ignore_list=[]
):
    brq_object_wrapper = {"header": {}, "narrativeRecords": [], "details": []}
    detail_records = {}
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(os.environ["EBOOKINGS_S3_TEMP_BUCKET"])
    brq_file_name = ""
    booking_request_id = ""
    line_data = ""
    from_email = ""
    pdf_attached = "No"
    for obj in bucket.objects.filter(Prefix=brq_file_name_list):
        if obj.key not in ignore_list:
            if obj.key.endswith(".brq"):
                brq_file_name = obj.key
                (from_email, booking_request_id, brq_file_name) = resolve_brq_file_name(
                    brq_file_name, context, event_id, brq_type
                )
                line_count = 0
                for line in obj.get()["Body"].read().decode("utf-8").splitlines():
                    if line_count < 1:
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
                                    if brq_detail_records_slice_config[key][1] == None:
                                        detail_records[key] = santize_brq_line_data(
                                            key,
                                            line[
                                                brq_detail_records_slice_config[key][
                                                    0
                                                ] :
                                            ].strip(),
                                        )
                                    else:
                                        detail_records[key] = santize_brq_line_data(
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
                                brq_object_wrapper["details"].append(detail_records)
                                detail_records = {}
                            else:
                                line_data = line.strip()
                                brq_object_wrapper["narrativeRecords"].append(line_data)
                                line_data = ""
                    line_count += 1
            else:
                if obj.key.endswith(".pdf"):
                    pdf_attached = "Yes"

    return (
        json.dumps(brq_object_wrapper),
        brq_file_name,
        booking_request_id,
        from_email,
        pdf_attached,
    )


def push_file_to_temp_s3(file_prefix, file_data, bucket_name):
    filename = file_prefix + ".json"
    s3 = boto3.resource("s3")
    s3.Object(bucket_name, filename).put(Body=file_data)


def update_sf_opportunity(payload, opp_id):
    payload["method"] = "UPDATE"
    payload["oppId"] = opp_id
    try:
        invoke_response = lambda_client.invoke(
            FunctionName=ARN_SF_ADAPTOR,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        custom_logger.info(f" Opp Update response: {invoke_response}")
        return json.loads(invoke_response["Payload"].read())
    except Exception as e:
        custom_logger.info(f" Error occured in Opp Update: {e}")
        return None


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


def get_current_week_sunday():
    today = date.today()
    days_to_last_sunday = (today.weekday() + 1) % 7
    last_sunday = today - timedelta(days=days_to_last_sunday)
    return last_sunday


def get_detail_records_summary(detail_records):
    record_summary = {}
    wc_dates = []
    req_spot_size = []
    demo_num = []
    overall_demo_num = []
    geo_list = []
    station_id_list = []
    geo_data = ""
    overall_budget = 0
    overall_spot_count = 0
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
        geo = get_geo_data(elem["StationId"])
        if elem["StationId"] != "" and elem["StationId"] not in station_id_list:
            station_id_list.append(elem["StationId"])
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
    custom_logger.info(record_summary)
    return record_summary


def get_geo_data(station_code):
    """
    Function to retrive geography information from CSV
    """
    try:
        geo = ""
        count = 0
        csv_data = get_csv_data(os.environ["SEIL_SALES_AREA_MAPPING_FILE"])
        for sub in csv_data:
            if sub["BCC"] == station_code:
                res_data = sub
                count += 1
        if count != 0:
            geo = res_data.get("Geography")
        return geo
    except Exception as e:
        custom_logger.info(f"Error returning geography details: {e}")


def lambda_handler(event, context):
    event_id = event["id"]
    custom_logger.info(
        f"Salesforce Opportunity creation started",
        context,
        correlationId=event_id,
        data=event,
    )
    try:
        common_utils = CommonUtils(event, custom_logger, context)
        key = event["parseBrqFile"]["response"]["key"]
        brq_file_name = event["parseBrqFile"]["response"]["brq_file_name"]
        booking_request_id = event["parseBrqFile"]["response"]["booking_request_id"]
        from_email = event["parseBrqFile"]["response"]["from_email"]
        pdf_attached = event["parseBrqFile"]["response"]["pdf_attached"]
        receive_time = receive_brq_time(key)
        record_summary = event["record_summary"]
        validation_response = event["validation_response"]
        brq_json_data = read_file_from_s3(
            os.environ["EBOOKINGS_S3_TEMP_BUCKET"], brq_file_name + ".json"
        )
        if validation_response["validationResult"]["result"] != "ERROR":
            if validation_response["brqSplit"] == "YES":
                opp_reponse = {"parent": None, "childOne": None, "childTwo": None}
                parent_ignore_list = [
                    validation_response["childBrqTwoPath"],
                    validation_response["childBrqOnePath"],
                ]
                child_ignore_list = (
                    validation_response["brqDirPath"] + "/" + from_email + "_"
                    "" + validation_response["brqFileName"] + ".brq"
                )
                sf_parent_opp_response_payload = prepare_sf_opp_payload(
                    event,
                    brq_json_data,
                    brq_file_name,
                    booking_request_id,
                    pdf_attached,
                    receive_time,
                    record_summary,
                    validation_response,
                    "PARENT",
                )
                if validation_response["oppStage"] == "Initiated":
                    sf_parent_opp_update_response = update_sf_opportunity(
                        sf_parent_opp_response_payload, validation_response["oppId"]
                    )
                    custom_logger.info(
                        f"SF parent opp update response: {sf_parent_opp_update_response}",
                        context,
                        correlationId=event_id,
                    )
                    opp_reponse = sf_parent_opp_update_response
                    opp_id = sf_parent_opp_update_response["id"]
                    if sf_parent_opp_update_response["success"] is True:
                        initiate_post_opp_creation_steps(
                            opp_id, key, parent_ignore_list
                        )
                else:
                    custom_logger.info(
                        f"Creating split Opp in SF",
                        context,
                        correlationId=event_id,
                    )
                    # Create parent Opp when BRQ Split case is TRUE

                    sf_parent_opp_response = create_sf_opportunity(
                        sf_parent_opp_response_payload
                    )
                    parent_opp_id = sf_parent_opp_response["id"]
                    if sf_parent_opp_response["success"] is True:
                        opp_reponse["parent"] = sf_parent_opp_response
                        custom_logger.info(
                            f"3. SF parent opp response: {sf_parent_opp_response}",
                            context,
                            correlationId=event_id,
                        )
                        initiate_post_opp_creation_steps(
                            parent_opp_id, key, parent_ignore_list
                        )

                # Create child Opps when BRQ Split case is TRUE
                (
                    brq_json_data,
                    brq_file_name,
                    booking_request_id,
                    from_email,
                    pdf_attached,
                ) = prepare_brq_json(
                    context,
                    event_id,
                    key + "/",
                    "CHILD",
                    [child_ignore_list, validation_response["childBrqTwoPath"]],
                )
                push_file_to_temp_s3(
                    brq_file_name, brq_json_data, os.environ["EBOOKINGS_S3_TEMP_BUCKET"]
                )
                brq_json_data = json.loads(brq_json_data)
                record_summary = get_detail_records_summary(
                    brq_json_data["details"]
                )  # ToDo - fetch value from context
                sf_child_opp_response1_payload = prepare_sf_opp_payload(
                    event,
                    brq_json_data,
                    brq_file_name,
                    booking_request_id,
                    pdf_attached,
                    receive_time,
                    record_summary,
                    validation_response,
                    "CHILD",
                    parent_opp_id,
                )
                sf_child_opp_response1 = create_sf_opportunity(
                    sf_child_opp_response1_payload
                )
                opp_id = sf_child_opp_response1["id"]
                if sf_child_opp_response1["success"] is True:
                    opp_reponse["childOne"] = sf_child_opp_response1
                    custom_logger.info(
                        f"3. SF child opp 1 response: {sf_child_opp_response1}",
                        context,
                        correlationId=event_id,
                    )
                    initiate_post_opp_creation_steps(
                        opp_id,
                        key,
                        [validation_response["childBrqTwoPath"], child_ignore_list],
                    )

                (
                    brq_json_data,
                    brq_file_name,
                    booking_request_id,
                    from_email,
                    pdf_attached,
                ) = prepare_brq_json(
                    context,
                    event_id,
                    key + "/",
                    "CHILD",
                    [child_ignore_list, validation_response["childBrqOnePath"]],
                )
                push_file_to_temp_s3(
                    brq_file_name, brq_json_data, os.environ["EBOOKINGS_S3_TEMP_BUCKET"]
                )
                brq_json_data = json.loads(brq_json_data)
                record_summary = get_detail_records_summary(
                    brq_json_data["details"]
                )  # ToDo - fetch value from context
                sf_child_opp_response2_payload = prepare_sf_opp_payload(
                    event,
                    brq_json_data,
                    brq_file_name,
                    booking_request_id,
                    pdf_attached,
                    receive_time,
                    record_summary,
                    validation_response,
                    "CHILD",
                    parent_opp_id,
                )
                sf_child_opp_response2 = create_sf_opportunity(
                    sf_child_opp_response2_payload
                )
                opp_id = sf_child_opp_response2["id"]
                if sf_child_opp_response2["success"] is True:
                    opp_reponse["childTwo"] = sf_child_opp_response2
                    custom_logger.info(
                        f"3. SF child opp 2 response: {sf_child_opp_response2}",
                        context,
                        correlationId=event_id,
                    )
                    initiate_post_opp_creation_steps(
                        opp_id,
                        key,
                        [validation_response["childBrqOnePath"], child_ignore_list],
                    )
                custom_logger.info(
                    f"Final opp response: {opp_reponse}",
                    context,
                    correlationId=event_id,
                )
                if (
                    opp_reponse["parent"]["success"] == True
                    and opp_reponse["childOne"]["success"] == True
                    and opp_reponse["childTwo"]["success"] == True
                ):
                    payload = {
                        "entity": "Opportunity",
                        "invocationType": "SOBJECTS",
                        "brqid": "",
                        "payload": {"SWM_Split_Childs_Created__c": True},
                    }
                    sf_opp_update_response = update_sf_opportunity(
                        payload, opp_reponse["parent"]["id"]
                    )
                    custom_logger.info(
                        f"SWM_Split_Childs_Created__c - attribute update response: {sf_opp_update_response}",
                        context,
                        correlationId=event_id,
                    )
            else:
                if validation_response["oppStage"] == "In Progress":
                    custom_logger.info(
                        f"Updating normal Opp in SF",
                        context,
                        correlationId=event_id,
                    )
                    payload = prepare_sf_opp_payload(
                        event,
                        brq_json_data,
                        brq_file_name,
                        booking_request_id,
                        pdf_attached,
                        receive_time,
                        record_summary,
                        validation_response,
                    )
                    sf_opp_response = update_sf_opportunity(
                        payload, validation_response["oppId"]
                    )
                    custom_logger.info(
                        f"SF opp update response: {sf_opp_response}",
                        context,
                        correlationId=event_id,
                    )
                    opp_reponse = sf_opp_response
                    opp_id = opp_reponse["id"]
                    if opp_reponse["success"]:
                        initiate_post_opp_creation_steps(opp_id, key)
                else:
                    custom_logger.info(
                        f"Creating opportunity in SF",
                        context,
                        correlationId=event_id,
                    )
                    payload = prepare_sf_opp_payload(
                        event,
                        brq_json_data,
                        brq_file_name,
                        booking_request_id,
                        pdf_attached,
                        receive_time,
                        record_summary,
                        validation_response,
                    )
                    sf_opp_response = create_sf_opportunity(payload)
                    custom_logger.info(
                        f"Opportunity creation response",
                        context,
                        correlationId=event_id,
                        data=sf_opp_response,
                    )
                    opp_reponse = sf_opp_response
                    opp_id = opp_reponse["id"]
                    if (
                        "message" in opp_reponse
                        and opp_reponse["message"]
                        == "There are no active sales patches for this opportunity. Please contact your Sales Insights team."
                    ):
                        event = {"brqDirPath": key}
                        description = "Account does not have an active sales patch for the duration of the campaign"
                        common_utils.create_sf_case(brq_file_name, description, event)
                    if opp_reponse["success"]:
                        initiate_post_opp_creation_steps(opp_id, key)
            return {"createSfOpp": {"response": opp_reponse}}
            # custom_logger.info(f"3. SF opp creation response: {sf_opp_response}")
    except UnicodeDecodeError as e:
        custom_logger.info(event)
        custom_logger.info(f"Got into UnicodeDecode Error exception : {e}")
        subject = "BRQ is corrupt"
        body = "The BRQ sent through is corrupt. Please refer to attached email and follow up with Agency/Direct Client."
        event = {"brqDirPath": key}
        common_utils.send_email_notification(from_email, subject, body, event, [".eml"])
        time.sleep(15)
        common_utils.move_file_s3_to_s3(
            os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
            os.environ["EBOOKINGS_S3_ERROR_BUCKET"],
            os.environ["EBOOKINGS_S3_FILEIN_BUCKET"],
            key,
            brq_file_name,
        )
    except (RuntimeError, KeyError, ValueError) as e:
        custom_logger.info(f"Got into Runtime/Key Error exception : {e}")
        common_utils.move_file_s3_to_s3(
            os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
            os.environ["EBOOKINGS_S3_ERROR_BUCKET"],
            os.environ["EBOOKINGS_S3_FILEIN_BUCKET"],
            key,
            brq_file_name,
        )
