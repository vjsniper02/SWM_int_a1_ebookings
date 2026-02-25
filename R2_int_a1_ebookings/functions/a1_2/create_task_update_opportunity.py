import os
from datetime import datetime
import boto3
import json


def lambda_handler(event, context):
    handler = CreateTaskUpdateOpportunityHandler(event)
    result = handler.handle()
    event.update(result)
    return event


TASK_SUBJECT = "Call"
TASK_TYPE = "Call"


class CreateTaskUpdateOpportunityHandler:
    def __init__(self, event) -> None:
        self.event = event

        self.report_path = event["spotPrebookingReport"]
        self.spot_result_status = event["spotPrebookingResultStatus"]
        self.spot_result_status_filtered = self.get_prebooking_status(
            self.spot_result_status
        )
        self.opportunity_id = event["detail"]["sf_payload"]["sf"]["opportunityID"]
        self.file_name = event["brqFileName"]

        self.salesforce_adaptor = os.environ["SALESFORCE_ADAPTOR"]

    def handle(self):
        return self.__create_task_in_sf()

    def get_prebooking_status(self, status_list):
        if "No" in status_list:
            return "No"
        elif all(s == "Yes" for s in status_list):
            return "Yes"
        elif "Partial" in status_list:
            return "Partial"
        else:
            return "Unknown"

    def __create_task_in_sf(self):
        today = datetime.now().strftime(r"%Y-%m-%d")

        invocation_data = {
            "invocationType": "COMPOSITE",
            "payload": {
                "allOrNone": True,
                "compositeRequest": [
                    {
                        "method": "POST",
                        "url": "/services/data/v58.0/sobjects/Task",
                        "referenceId": "refTask",
                        "body": {
                            "Subject": TASK_SUBJECT,
                            "Description": f"BRQ spot '{self.file_name}' has been loaded and the status is '{self.spot_result_status_filtered}'",
                            "whatId": self.opportunity_id,  # opportunity ID
                            "ActivityDate": today,  # today
                            # "CompletedDateTime": "",
                            # "Type": TASK_TYPE
                        },
                    },
                    {
                        "method": "PATCH",
                        "url": f"/services/data/v58.0/sobjects/Opportunity/{self.opportunity_id}",
                        "referenceId": "refOpp",
                        "body": {
                            "SWM_Spots_created__c": self.spot_result_status_filtered
                        },
                    },
                    {
                        "method": "PATCH",
                        "url": f"/services/data/v58.0/sobjects/Opportunity/{self.opportunity_id}",
                        "referenceId": "refOpp2",
                        "body": {"SWM_Error_Message__c": ""},
                    },
                ],
            },
        }

        lambda_client = boto3.client("lambda")
        lambda_response = lambda_client.invoke(
            FunctionName=self.salesforce_adaptor,
            InvocationType="RequestResponse",
            Payload=json.dumps(invocation_data),
        )

        print("Salesforce response")
        print(lambda_response)

        sf_response_json = json.loads(lambda_response["Payload"].read())
        print("Salesforce response body")
        print(sf_response_json)

        task_id = sf_response_json["compositeResponse"][0]["body"]["id"]
        self.task_id = task_id

        return {"salesforceTaskID": self.task_id}
