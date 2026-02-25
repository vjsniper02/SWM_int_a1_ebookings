import os
from datetime import datetime
import boto3
import json


def lambda_handler(event, context):
    # The report detail is documented in the JIRA ticket: https://code7plus.atlassian.net/browse/R2DEV-1758

    handler = UploadReportHandler(event)
    result = handler.handle()
    event.update(result)

    # return event to pass to next step in the state machine
    return event


class UploadReportHandler:
    def __init__(self, event) -> None:
        self.event = event

        self.report_path = event["spotPrebookingReport"]
        self.opportunity_id = event["detail"]["sf_payload"]["sf"]["opportunityID"]
        self.task_id = event["salesforceTaskID"]
        self.salesforce_adaptor = os.environ["SALESFORCE_ADAPTOR"]
        self.file_name = event["brqFileName"]

        self.salesforce_adaptor = os.environ["SALESFORCE_ADAPTOR"]

    def get_prebooking_status(self, status_list):
        if "No" in status_list:
            return "No"
        elif all(s == "Yes" for s in status_list):
            return "Yes"
        elif "Partial" in status_list:
            return "Partial"
        else:
            return "Unknown"
        
    def handle(self):
        self.spotPrebookingResultStatus = self.get_prebooking_status(self.event["spotPrebookingResultStatus"])
        if self.spotPrebookingResultStatus == "Yes":
            print(
                "All spots are successfully booked, no file will be attached to the Task and Opportunity."
            )
            return {}
        # Get iteration count and payload list from event
        iteration_count = int(self.event.get("trancheFileCount", 1))
        report_list = self.event.get("spotPrebookingReport", [])
        
        spotPrebookingResponsePathList = []
        spotPrebookingResponseCodeList = []
        spotPrebookingStatusList = []

        # Loop through iteration count
        for i in range(iteration_count):
            if i >= len(report_list):
                raise IndexError(f"Index {i} out of range for report list")

            report_value = report_list[i]

            file_name = os.path.basename(report_value)
            self.upload_file_to_task_response = self.__upload_to_file(
                self.task_id, file_name, report_value
            )
            self.upload_file_to_opportunity_response = self.__upload_to_file(
                self.opportunity_id, file_name, report_value
            )

        return {
            "uploadFileToTaskResponse": self.upload_file_to_task_response,
            "uploadFileToOpportunityResponse": self.upload_file_to_opportunity_response,
        }

    def __upload_to_file(self, record_id, file_name, file_uri):
        invocation_data = {
            "invocationType": "UPLOADFILE",
            "record_id": record_id,
            "file_uri": file_uri,
            "extra_header": {
                "filename": file_name,
                "NEILON__Category__c": "Other",  # TODO Change to "E-Booking Request" after the ticket R2DEV-2225 fixed
            },
        }

        lambda_client = boto3.client("lambda")
        lambda_invoke_response = lambda_client.invoke(
            FunctionName=self.salesforce_adaptor,
            InvocationType="RequestResponse",
            Payload=json.dumps(invocation_data),
        )

        print(lambda_invoke_response)
        response_body = lambda_invoke_response["Payload"].read()
        print(response_body)

        return json.loads(response_body)
