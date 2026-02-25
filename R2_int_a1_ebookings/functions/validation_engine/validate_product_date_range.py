import logging
import boto3
import os
import json
from boto3 import client as boto3_client
from functions.common_utils import CommonUtils
from swm_logger.swm_common_logger import LambdaLogger

custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])


def get_ssm(param_name):
    """
    function to feth ssm values from AWS param store
    """
    ssm_client = boto3.client("ssm")
    param_value = (
        ssm_client.get_parameter(Name=param_name).get("Parameter").get("Value")
    )
    return param_value


def get_lmk_product_details(event):
    """
    Function to Invoke Landmark Adaptor
    """
    custom_logger.info(f"ENTRY Invoke Lambda Adaptor")
    client = boto3.client("lambda")

    lmk_base_url = get_ssm(os.environ["LANDMARK_BASE_URL"])
    lmk_adaptor = get_ssm(os.environ["LANDMARK_ADAPTOR_FUNCTION"])
    interface_number = os.environ["LANDMARK_INTERFACE_NUMBER"]

    # Append product service to base URL
    lmk_product_url = lmk_base_url + "Products"
    custom_logger.info(lmk_product_url)
    lmk_product_url = (
        lmk_product_url
        + f"?startDate={event['brqWcStartDate']}&endDate={event['brqWcEndDate']}&externalCode={event['brqClientProductId']}&interfaceNumber={interface_number}"
    )
    custom_logger.info(f"Landmark URL: {lmk_product_url}")

    # Landmark Header with authenticatin token
    lmk_request = {
        "landmarkEndpoint": lmk_product_url,
        "content": "",
        "operation": "get",
    }

    # Invoke Landmark Adapatar Lambda function using boto3 invoke method
    try:
        custom_logger.info(f"Invoke Landmark Adapator for A1 product check")
        lmk_response = client.invoke(
            FunctionName=lmk_adaptor,
            InvocationType="RequestResponse",
            Payload=json.dumps(lmk_request),
        )
        custom_logger.info(lmk_response)
        lmk_raw_response = lmk_response["Payload"].read()
        lmk_raw_response = json.loads(lmk_raw_response.decode())
        custom_logger.info(f"LMK raw response: {lmk_raw_response}")
        if type(lmk_raw_response) is not list and lmk_raw_response in [
            503,
            500,
            502,
            504,
        ]:
            custom_logger.info("Landmark is Unavailable")
            status_code = lmk_raw_response
            lmk_resp = {}
        else:
            if lmk_raw_response == []:
                lmk_resp = []
                status_code = "200"
            else:
                custom_logger.info("Landmark product data retrival sucessful")
                lmk_resp = lmk_raw_response
                status_code = "200"
        return {"statusCode": status_code, "data": lmk_resp}
    except Exception as e:
        custom_logger.error(f"Error in Landmark Adapator Invoke - {e}")


def lambda_handler(event, context):
    """
    Funtion to get product data from LMK
    """
    common_utils = CommonUtils(event, custom_logger, context)
    if event["validationResult"]["continueValidation"] is True:
        custom_logger.info(f"event_data: {event}")
        validation_status = "SUCCESS"
        continue_validation = True
        lmk_product_response = get_lmk_product_details(event)
        custom_logger.info(
            f"Landmark product response post mapping: {lmk_product_response}"
        )
        lmk_status_code = lmk_product_response["statusCode"]
        if lmk_status_code not in [503, 500, 502, 504]:
            if (
                lmk_status_code == "200" and lmk_product_response["data"] == []
            ) or lmk_status_code == "400":
                description = f"[{event['brqClientProductId']}], [{event['brqClientProductName']}] does not exist in Landmark within the Campaign Dates."
                custom_logger.info(f"Product does not exist in LMK")
                validation_status = "WARNING"
                event["lmkProductResponse"] = {}
                # event["validationMessages"].append(
                #     f"[{event['brqClientProductId']}], [{event['brqClientProductName']}] does not exist in Landmark within the Campaign Dates. Please search for the Product in Landmark."
                # )
                title = "Product Not Found"
                first_line = "This EBooking Case has been raised due to the BRQ Product not matching Landmark"
                case_response = common_utils.create_sf_case_product_notmatch(
                    event["brqFileName"],
                    description,
                    event,
                    first_line,
                    title,
                )
                if (
                    "id" in case_response
                    and case_response["id"]
                    and case_response["success"] is True
                ):
                    event["validationMessages"].append(
                        "A Product Not Matched Case has been raised and assigned to Sales Insights. They will reach out to you to ensure they have all the necessary information to create the product."
                    )
            else:
                custom_logger.info(
                    f"LMk status == 200, setting landmark values (productCode and productName) on SF Opp"
                )
                event["lmkProductResponse"] = lmk_product_response["data"][0]
                validation_status = "SUCCESS"
            event["validationResult"]["result"] = validation_status
            event["validationResult"]["continueValidation"] = continue_validation
            event["validationResult"]["details"].append(
                {
                    "ruleName": "Validate Product date range in LMK",
                    "result": validation_status,
                    "notificationTo": "",
                    "msg": "",
                }
            )
    return event
