import logging
import os
import boto3
import json
import re

from functions.BRQParser import BRQParser


def lambda_handler(event, context):
    """
    Based on the document of https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138/A1+-+eBooking+Detailed+Design#Feature-8---EBooking-Spots-Preprocessing-State-Machine
    The Preprocessing State Machine input is
    {
        "version": "0",
        "id": "71f18a84-7c6a-cd0e-6f53-8ddc2c10983d",
        "detail-type": "seil.landmark.campaign.created",
        "source": "seil.landmark.campaign.created",
        "account": "<REDACTED_AWS_ACCOUNT_ID>",
        "time": "2024-01-18T19:53:19Z",
        "region": "ap-southeast-2",
        "resources": [],
        "detail": { // detail is the message detail
            "sf_payload": {
                "sf": {
                    "integrationJobID": "The Salesforce integration Job that SEIL will respond back to Salesforce",
                    "opportunityID"": "The Salesforce Opportunity ID",
                    "opportunityOwnerID": "The Salesforce Opportunity Ownwer ID",
                    "recordTypeDeveloperName": "The Developer Name of the Record Type, e.g. Broadcast_Campaign_E_Booking",
                    "STPCSVFilePath"": "The path fo the Sales Planning Tool CSV File path in the Creative Media bucket, e.g. s3://{bucketname}/{opportunity_folder}/{file_name}"
                },
                "spotBookingFilePath": "string",
                "approvalVersionNumber": 0,
                "approvalKeyID": 0,
                "demographNumber": 0,
                "dealNumber": 0,
                "productCode": 0,
                "revenueBudget": 0,
                ... all other fields to create campaign in LMK ...
            }
        }
    }

    Output is

    {
        ... all fields from the event ...
        "brqJsonBucket": "",
        "brqJsonKey": "",
        "brqFileName": ""
    }

    This lambda function
    1. Query the BRQ file from Salesforce API
    2. Read the latest BRQ file from Creative Media bucket
    3. Parse the BRQ file to BRQ JSON
    4. Write the BRQ JSON to EBooking Temp Bucket
    5. Return the Bucket and Key of the BRQ JSON file
    """

    handler = GetBRQFileHandler(event)
    output_event = handler.handle()

    # merge the input event and output_event
    event.update(output_event)

    # return event to pass to next step in the state machine
    return event


class GetBRQFileHandler:

    def __init__(self, event):
        self.event = event
        self.correlation_id = self.event["id"]  # Event ID
        self.opportunity_id = self.event["detail"]["sf_payload"]["sf"]["opportunityID"]

        self.in_bucket_name = os.environ["EBOOKINGS_S3_FILEIN_BUCKET"]
        self.temp_bucket_name = os.environ["EBOOKINGS_S3_TEMP_BUCKET"]
        self.seil_config_bucket_name = os.environ["SEIL_CONFIG_BUCKET_NAME"]
        self.region = os.environ["SEIL_AWS_REGION"]
        self.raw_brq_file_name = (
            ""  # will be resolved in the __get_latest_brq_file_info method
        )

        # prepare the logger with correlation ID
        self.logger = logging.getLogger("get_brq_file.lambda_handler")
        self.logger.__format__ = logging.Formatter(
            self.correlation_id + " - %(message)s"
        )

        self.logger.info("The input event of the get_brq_file.lambda_handler")
        self.logger.info(self.event)

    def handle(self):
        brq_file_record = self.__get_latest_brq_file_info(self.opportunity_id)
        brq_file_content = self.__read_brq_file_content(brq_file_record)
        brq_object = self.__parse_brq_file(brq_file_content)
        (bucket, key) = self.__write_temp_bucket(brq_object)
        self.event.update(
            {
                "brqJsonBucket": bucket,
                "brqJsonKey": key,
                "brqFileName": self.raw_brq_file_name,
                "brqRequestID": self.__get_request_id_from_file_name(
                    self.raw_brq_file_name
                ),
            }
        )
        return self.event

    def __get_request_id_from_file_name(self, brq_file_name):
        if brq_file_name.endswith(".brq"):
            matches = re.search(
                r"-Request-([A-Za-z0-9\-_]+)\.brq$", brq_file_name, re.IGNORECASE
            )  # all char between "Request-" and ".brq"
        else:
            matches = re.search(
                r"-Request-([A-Za-z0-9\-_]+)\.zip$", brq_file_name, re.IGNORECASE
            )  # all char between "Request-" and ".brq"

        if matches:
            file_name_with_out_brq = matches[1].replace("brq", "")
            return file_name_with_out_brq

    def __parse_brq_file(self, brq_file_content) -> dict:
        parser = BRQParser(brq_file_content)
        lines = brq_file_content.splitlines()
        self.logger.debug(brq_file_content)
        brq_object = parser.parse()
        if parser.has_error():
            raise Exception("BRQ Parse error: " + str(parser.get_error()))
        return brq_object

    def __write_temp_bucket(self, brq_object) -> dict:
        s3client = boto3.client("s3", region_name=self.region)
        brq_json_string = json.dumps(brq_object)
        file_key = self.correlation_id + "/" + self.raw_brq_file_name + ".json"
        s3client.put_object(
            Bucket=self.temp_bucket_name,
            Key=file_key,
            Body=brq_json_string.encode("utf-8"),
        )
        return (self.temp_bucket_name, file_key)

    def __read_brq_file_content(self, brq_file_info) -> str:
        """
        This method read the BRQ file from Creative Media bucket through S3 Link API
        """
        salesforce_adaptor_arn = os.environ["SALESFORCE_ADAPTOR"]
        lambda_client = boto3.client("lambda", region_name=self.region)
        lambda_invoke_response = lambda_client.invoke(
            FunctionName=salesforce_adaptor_arn,
            InvocationType="RequestResponse",
            Payload=json.dumps(
                {
                    "invocationType": "S3LINK_READFILECONTENT",
                    "record_id": brq_file_info["record_id"],
                    "type": "text",
                    "bucket_name": self.temp_bucket_name,
                    "brq_zip_flag": brq_file_info["brq_zip_flag"],
                }
            ),
        )
        # brq_file_content = lambda_invoke_response["Payload"].read().decode("utf-8-sig")
        s3_details = lambda_invoke_response["Payload"].read().decode("utf-8-sig")
        s3_json_data = json.loads(s3_details)
        print("s3_json_data: ", s3_json_data)
        s3 = boto3.client("s3")
        response = s3.get_object(
            Bucket=s3_json_data["bucket_name"], Key=s3_json_data["file_name"]
        )
        print("response: ", response)
        brq_file_content = response["Body"].read().decode("utf-8-sig")
        print("brq_file_content: ", brq_file_content)
        brq_file_content = brq_file_content.strip(
            '"'
        )  # remove the front and the end double quote
        brq_file_content = brq_file_content.replace("\\n", "\n")  # special handling
        brq_file_content = brq_file_content.replace("\\r", "")  # remove \r
        print("brq_file_content")
        print(brq_file_content)
        return brq_file_content

    def __get_latest_brq_file_info(self, opportunity_id: str) -> dict:
        """
        Invoke the SALESFORCE_ADAPTOR to get the Neilson files
        Based on the design (https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138/A1+-+eBooking+Detailed+Design#Get-BRQ-File-By-Opportunity-ID-Or-Case-ID)
        , the query should be
        SELECT FIELDS(ALL) FROM NEILON__File__c WHERE NEILON__Category__c = 'E-Booking Request' AND NEILON__Opportunity__c = opportunityID

        The return object of this method is {"Bucket":"bucket-name", "Key":"file-key"}
        """
        salesforce_adaptor_arn = os.environ["SALESFORCE_ADAPTOR"]

        self.logger.debug(
            f"Getting Salesforce file list by opportunity: {opportunity_id}..."
        )

        lambda_client = boto3.client("lambda", region_name=self.region)
        lambda_invoke_response = lambda_client.invoke(
            FunctionName=salesforce_adaptor_arn,
            InvocationType="RequestResponse",
            Payload=json.dumps(
                {
                    "invocationType": "QUERY",
                    "query": "SELECT+FIELDS(ALL)+FROM+NEILON__File__c+WHERE+(NEILON__Category__c='E-Booking Request'+OR+NEILON__Category__c='EBookings Request')"
                    + f"+AND+NEILON__Opportunity__c='{opportunity_id}'"
                    + "+AND+(+NEILON__Extension__c='.brq'"
                    + "+OR+Name+LIKE+'%brq.zip'+)"
                    + "+ORDER+BY+NEILON__Last_Replaced_Date__c+DESC+NULLS+LAST+LIMIT+1",
                }
            ),
        )
        sf_file_list = json.loads(lambda_invoke_response["Payload"].read())
        self.logger.info("File list from Salesforce")
        self.logger.info(sf_file_list)
        print("File list from Salesforce")
        print(sf_file_list)

        # check if any brq file exists
        if "records" not in sf_file_list or len(sf_file_list["records"]) == 0:
            raise Exception(
                f"No E-Booking BRQ file has been found for opportunity {opportunity_id}"
            )

        file_record_id = sf_file_list["records"][0]["Id"]
        amazon_file_key = sf_file_list["records"][0]["NEILON__Amazon_File_Key__c"]
        if amazon_file_key.endswith("brq.zip"):
            brq_zip_flag = True
        else:
            brq_zip_flag = False

        brq_file = sf_file_list["records"][
            0
        ]  # the SOQL used the ORDER BY and LIMIT to ensure the first 1 is the latest one
        self.raw_brq_file_name = brq_file["Name"]
        return {
            "Bucket": brq_file["NEILON__Bucket_Name__c"],
            "Region": brq_file["NEILON__Bucket_Region__c"],
            "Key": brq_file["NEILON__Amazon_File_Key__c"],
            "record_id": file_record_id,
            "brq_zip_flag": brq_zip_flag,
        }
