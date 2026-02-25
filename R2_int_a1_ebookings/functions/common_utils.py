import logging
import json
import csv
import os
import datetime as dt
import boto3
import zipfile
import io
from io import BytesIO
import botocore.exceptions
from datetime import date, datetime, timedelta
import time
from calendar import monthcalendar

from datetime import datetime
from boto3 import client as boto3_client, resource

email_msgs = json.load(
    open(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__), ".", "email_msg_config.json")
        )
    )
)


class CommonUtils:

    def __init__(self, event, custom_logger, context=None):
        self.custom_logger = custom_logger
        self.event = event
        self.context = context
        self.region = os.environ["SEIL_AWS_REGION"]
        self.CEE_NOTIFICATION_ENGINE = os.environ["CEE_NOTIFICATION_ENGINE"]
        self.ARN_SF_ADAPTOR = os.environ["SALESFORCE_ADAPTOR"]
        self.lambda_client = boto3_client("lambda", region_name=self.region)
        self.step_function = boto3_client("stepfunctions", region_name=self.region)

    def extract_move_brq_file(
        self, filekey, source_bucket_name, err_bucket, brq_file_name
    ):
        try:
            s3_resource = resource("s3")
            err_bucket = s3_resource.Bucket(err_bucket)
            self.custom_logger.info(
                f"unzipping file of {source_bucket_name}, {filekey} to {err_bucket} "
            )
            zipped_file = s3_resource.Object(
                bucket_name=source_bucket_name, key=filekey
            )
            self.custom_logger.info(f"Zip file object: {zipped_file}")
            buffer = BytesIO(zipped_file.get()["Body"].read())
            zipped = zipfile.ZipFile(buffer)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            for file in zipped.namelist():
                file_name = os.path.basename(file)

                # split the file name and extension
                file_root, file_extension = os.path.splitext(file_name)

                # Append timestamp to the filename to make it unique
                new_file_name = f"{file_root}_{timestamp}{file_extension}"

                self.custom_logger.info(f"current file in zipfile: {file}")
                final_file_path = new_file_name

                with zipped.open(file, "r") as f_in:
                    unzipped_content = f_in.read()
                    err_bucket.upload_fileobj(
                        io.BytesIO(unzipped_content),
                        final_file_path,
                        ExtraArgs={"ContentType": "text/plain"},
                    )
        except Exception as e:
            self.custom_logger.info(f"Error: Unable to gzip & upload file: {e}")

    def move_file_s3_to_s3(
        self,
        source_bucket: object,
        destination_bucket: object,
        file_in_source_bucket: object,
        source_prefix: str,
        brq_file_name: str,
    ):
        """Funtion to move files from ne s3 to other"""
        self.custom_logger.info(f"Moving file to Ebooking error bucket")
        s3 = boto3.client("s3")

        # List objects in the source S3 folder
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

        # If no files found in the source folder
        if "Contents" not in response:
            self.custom_logger.info("No files found in the source folder.")
            return

        self.extract_move_brq_file(
            source_prefix, file_in_source_bucket, destination_bucket, brq_file_name
        )

        # Iterate through all files in the source folder
        for obj in response["Contents"]:
            source_key = obj["Key"]

            # Copy the object to the destination bucket with the new file name
            # copy_source = {"Bucket": source_bucket, "Key": source_key}
            # s3.copy(copy_source, destination_bucket, new_file_name)
            # self.custom_logger.info(f"Copied: {source_key} to {new_file_name}")

            # Delete the object from the source bucket
            s3.delete_object(Bucket=source_bucket, Key=source_key)
            self.custom_logger.info(f"Deleted: {source_key} from temp bucket")

        s3.delete_object(Bucket=source_bucket, Key=source_prefix + "/")
        self.custom_logger.info(f"Deleted: {source_prefix}/ folder from temp bucket")

        # Delete the object from the secondary source bucket
        s3.delete_object(Bucket=file_in_source_bucket, Key=source_prefix)
        self.custom_logger.info(f"Deleted: {source_prefix} from file-in bucket")

        self.custom_logger.info("Move operation completed.")

    def __generate_table_description(self, data):
        """Generates description with tabular data.
        :param data: Tabular data as a list of lists.
        :return: Formatted description."""
        description = ""
        for row in data:
            self.custom_logger.info(row)
            description += "|   ".join(row) + "\n"
        self.custom_logger.info(description)
        return description

    def __construct_s3_attachments(self, event):
        print(event)
        s3_attachments = []
        extension_list = [".brq", ".eml", ".pdf"]
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(os.environ["EBOOKINGS_S3_TEMP_BUCKET"])
        for extension in extension_list:
            for obj in bucket.objects.filter(Prefix=event["brqDirPath"]):
                if obj.key.endswith(extension):
                    file_name = obj.key.split("/")[1]
                    s3_attachments.append(
                        {
                            "name": file_name,
                            "bucket": os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
                            "key": obj.key,
                            "content_type": "text/plain",
                            "link": f"s3://{os.environ['EBOOKINGS_S3_TEMP_BUCKET']}/{obj.key}",
                        }
                    )
        return s3_attachments

    def get_ssm_parameter(self, parameter_name: str) -> str:
        """Function to retrive AWS SSM parameters"""
        try:
            ssm = boto3.client("ssm")
            response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
            return response["Parameter"]["Value"]
        except botocore.exceptions.ClientError as e:
            self.custom_logger.info(f"exception error in getting ssm param: {e}")
            return e

    def get_sf_recordtype_id(self, recordtype_name):
        payload = {
            "invocationType": "QUERY",
            "query": f"SELECT+Id+from+RecordType+WHERE+DeveloperName+='{recordtype_name}'+AND+SobjectType+='Case'+LIMIT+1",
        }
        try:
            invoke_response = self.lambda_client.invoke(
                FunctionName=self.ARN_SF_ADAPTOR,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            downstream_response = json.loads(invoke_response["Payload"].read())
            downstream_response = downstream_response["records"][0]["Id"]
            self.custom_logger.info(downstream_response)
            return downstream_response
        except Exception as e:
            self.custom_logger.info(f"Error getting salesforce Record Type Id: {e}")
            return False

    def get_sf_owner_id(self, email_id):
        payload = {
            "invocationType": "QUERY",
            "query": f"select+Id,Email,IsActive+from+user+where+email+='{email_id}'+and+IsActive+=true+LIMIT+1",
        }
        try:
            invoke_response = self.lambda_client.invoke(
                FunctionName=self.ARN_SF_ADAPTOR,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            downstream_response = json.loads(invoke_response["Payload"].read())
            downstream_response = downstream_response["records"][0]["Id"]
            self.custom_logger.info(downstream_response)
            return downstream_response
        except Exception as e:
            self.custom_logger.info(f"Error getting salesforce Record Type Id: {e}")
            return False

    def create_sf_case_SA_notmatch(self, brq_file_name, description, event):
        """
        Function to create salesforce case for Sales Area not match scenario.
        """
        self.custom_logger.info(f"SF case creation block")
        trasformed_network_name = ""
        s3_attachments = self.__construct_s3_attachments(event)
        if event["brqNetworkName"] == "SEVNET":
            trasformed_network_name = "7(Seven)"
        elif event["brqNetworkName"] == "PRIPRO":
            trasformed_network_name = "7PRIME"
        else:
            trasformed_network_name = event["brqNetworkName"]

        recotd_type_id = self.get_sf_recordtype_id(
            self.get_ssm_parameter(os.environ["SF_AD_SALES_RECORD_TYPE_NAME"])
        )

        table_data = [
            ["Network Name".ljust(19, "-"), trasformed_network_name],
            ["Sales User".ljust(21, "-"), event["brqEmail"]],
            ["BRQ Request ID".ljust(17, "-"), event["brqId"]],
            ["Agency ID".ljust(22, "-"), event["brqAgencyId"]],
            ["Agency Name".ljust(20, "-"), event["brqAgencyName"]],
            ["Advertiser ID".ljust(18, "-"), event["brqClientId"]],
            ["Advertiser Name".ljust(16, "-"), event["brqClientName"]],
            ["Product ID".ljust(21, "-"), event["brqClientProductId"]],
            ["Product Name".ljust(19, "-"), event["brqClientProductName"]],
        ]
        tabular_data = self.__generate_table_description(table_data)
        final_desc_data = f"This EBooking Case has been raised due to the BRQ containing Station Id’s that are not BCC Codes\n\n{tabular_data}\n\nIssues Identified\nPlease investigate the following issues:\n\nThe Following Station Id's did not match BBC codes in Sales area mapping table\n\n{description}\n\n"
        payload = {
            "invocationType": "CASE",
            "s3_attachments": s3_attachments,
            "data": {
                "RecordTypeId": recotd_type_id,
                "Case_Category__c": "EBooking",
                "Sub_Category__c": "EBooking",
                "Priority": "High",
                "Status": "In Progress",
                "Subject": f"This EBooking Case has been raised due to the BRQ containing Station Id’s that are not BCC Codes [{brq_file_name}]",  # TODO - Subject needs to be confirmed in user story
                "Description": f"{final_desc_data}",
                "OwnerId": self.get_ssm_parameter(
                    os.environ["EBOOKINGS_CASE_QUEUE_ID"]
                ),
            },
        }
        self.custom_logger.info(f"SF case creation initiated: {payload}")
        try:
            invoke_response = self.lambda_client.invoke(
                FunctionName=self.ARN_SF_ADAPTOR,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            downstream_response = json.loads(invoke_response["Payload"].read())
            self.custom_logger.info(f"SF case creation response: {downstream_response}")
            return downstream_response
        except Exception as e:
            self.custom_logger.info(f"Error on salesforce case creation: {e}")
            return False

    def create_sf_case_product_notmatch(
        self,
        brq_file_name,
        description,
        event,
        first_line,
        title="",
    ):
        """
        Function to create salesforce case for LMK product not match scenario.
        """
        trasformed_network_name = ""
        s3_attachments = self.__construct_s3_attachments(event)
        if event["brqNetworkName"] == "SEVNET":
            trasformed_network_name = "7(Seven)"
        elif event["brqNetworkName"] == "PRIPRO":
            trasformed_network_name = "7PRIME"
        else:
            trasformed_network_name = event["brqNetworkName"]

        recotd_type_id = self.get_sf_recordtype_id(
            self.get_ssm_parameter(os.environ["SF_AD_SALES_RECORD_TYPE_NAME"])
        )
        if title == "":
            title = f"The EBooking Product Not Matched with the BRQ [{brq_file_name}] data in Landmark"
        table_data = [
            ["Network Name".ljust(19, "-"), trasformed_network_name],
            ["Sales User".ljust(21, "-"), event["brqEmail"]],
            ["BRQ Request ID".ljust(17, "-"), event["brqId"]],
            ["Agency ID".ljust(22, "-"), event["brqAgencyId"]],
            ["Agency Name".ljust(20, "-"), event["brqAgencyName"]],
            ["Advertiser ID".ljust(18, "-"), event["brqClientId"]],
            ["Advertiser Name".ljust(16, "-"), event["brqClientName"]],
            ["Product ID".ljust(21, "-"), event["brqClientProductId"]],
            ["Product Name".ljust(19, "-"), event["brqClientProductName"]],
        ]
        tabular_data = self.__generate_table_description(table_data)
        final_desc_data = f"{first_line}\n\n{tabular_data}\n\nIssues Identified\nPlease investigate the following issues:\n\n{description}\n\n"
        payload = {
            "invocationType": "CASE",
            "s3_attachments": s3_attachments,
            "data": {
                "RecordTypeId": recotd_type_id,
                "Case_Category__c": "EBooking",
                "Sub_Category__c": "EBooking",
                "AccountId": event["sfAdvertiserAccountId"],
                "Priority": "High",
                "Status": "In Progress",
                "Subject": title,
                "Description": f"{final_desc_data}",
                "OwnerId": self.get_ssm_parameter(
                    os.environ["EBOOKINGS_CASE_QUEUE_ID"]
                ),
            },
        }
        self.custom_logger.info(f"SF case creation initiated: {payload}")
        try:
            invoke_response = self.lambda_client.invoke(
                FunctionName=self.ARN_SF_ADAPTOR,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            downstream_response = json.loads(invoke_response["Payload"].read())
            self.custom_logger.info(f"SF case creation response: {downstream_response}")
            return downstream_response
        except Exception as e:
            self.custom_logger.info(f"Error on salesforce case creation: {e}")
            return False

    def create_sf_case(self, brq_file_name, description, event):
        """
        Function to create salesforce case.
        """
        s3_attachments = self.__construct_s3_attachments(event)
        table_data = [
            ["Network Name".ljust(19, "-"), event["brqNetworkName"]],
            ["Sales Representative".ljust(21, "-"), event["brqEmail"]],
            ["BRQ Request ID".ljust(20, "-"), event["brqId"]],
        ]
        self.custom_logger.info(table_data)
        tabular_data = self.__generate_table_description(table_data)
        final_desc_data = f"This EBooking Case has been raised due to issues identified with the BRQ [{brq_file_name}] data in Salesforce\n\n{tabular_data}\n\nIssues Identified\nPlease investigate the following issues:\n\n{description}\n\nOnce all issues are correct, please resolve the case and resend the attached email to ETRANS. If the EBooking Request cannot be resent, please contact the Sales Representative defined above."
        payload = {
            "invocationType": "CASE",
            "s3_attachments": s3_attachments,
            "data": {
                "Case_Category__c": "EBooking",
                "Sub_Category__c": "EBooking",
                "Priority": "High",
                "Status": "In Progress",
                "Subject": f"This EBooking Case has been raised due to issues identified with the BRQ [{brq_file_name}] data in Salesforce",
                "Description": f"{final_desc_data}",
                "OwnerId": self.get_ssm_parameter(
                    os.environ["EBOOKINGS_CASE_QUEUE_ID"]
                ),
            },
        }
        self.custom_logger.info(f"SF case creation initiated: {payload}")
        try:
            invoke_response = self.lambda_client.invoke(
                FunctionName=self.ARN_SF_ADAPTOR,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            downstream_response = json.loads(invoke_response["Payload"].read())
            self.custom_logger.info(f"SF case creation response: {downstream_response}")
            return downstream_response
        except Exception as e:
            self.custom_logger.info(f"Error on salesforce case creation: {e}")
            return False

    def send_email_notification(
        self, recipients, subject, body, event, file_ext_list=[]
    ):
        payload = {
            "id": "234rf23f2fd32f34ty56hdfg34",
            "body": {
                "type": "Ebookings",
                "source": "ebooking-no-reply@test.code7swm.com.au",
                "emails": recipients,
                "subject": subject,
                "body": body,
            },
        }
        try:
            if len(file_ext_list) != 0:
                brq_file_name_list, child_brq_file_list, other_file_list = (
                    self.get_s3_files_for_extension(
                        os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
                        event["brqDirPath"] + "/",
                        file_ext_list,
                    )
                )
                if len(other_file_list) != 0:
                    eml_file_name = other_file_list[0].split("/")[1]
                    payload["body"]["files"] = [
                        {
                            "path": f"s3://{os.environ['EBOOKINGS_S3_TEMP_BUCKET']}/{event['brqDirPath']}/{eml_file_name}"
                        }
                    ]
            self.custom_logger.info(
                f"Email notification request payload to CEE: {payload}"
            )
            invoke_response = self.step_function.start_execution(
                stateMachineArn=self.CEE_NOTIFICATION_ENGINE,
                input=json.dumps(payload),
            )
            self.custom_logger.info(f"Email notification sent: {invoke_response}")
            return invoke_response
        except Exception as e:
            self.custom_logger.info(f"Error sending email notification: {e}")

    def get_s3_files_for_extension(self, bucket_name, file_prefix, extension_list):
        """Function get the list of files from S3 bucket based on the extension list provided on the function argument"""
        try:
            parent_files = []
            child_files = []
            other_files = []
            s3 = boto3.resource("s3")
            bucket = s3.Bucket(bucket_name)
            parent_delimiter = "_"
            for extension in extension_list:
                for obj in bucket.objects.filter(Prefix=file_prefix):
                    if obj.key.endswith(extension):
                        self.custom_logger.info(obj.key)
                        if extension == ".brq":
                            if parent_delimiter in obj.key:
                                self.custom_logger.info(f"parent {obj.key}")
                                parent_files.append(obj.key)
                            else:
                                child_files.append(obj.key)
                        else:
                            other_files.append(obj.key)
                    self.custom_logger.info(
                        f"files found for extension {extension_list} : Parent BRQ: {parent_files}, Child BRQ: {child_files}, Other Files: {other_files} on Bucket: {bucket_name} Directory: {file_prefix}"
                    )
            return parent_files, child_files, other_files
        except Exception as e:
            raise RuntimeError(
                f"Error retrieving files from S3 Bucket: '{bucket_name}' Directory: '{file_prefix}' : {e}"
            ) from e

    def __get_user_type_by_email(self, email_id):
        """
        Private Function to get the user type based on the email domain
        """
        user_type = ""
        domain = email_id.split("@")
        try:
            if str(domain[1]) in ["seven.com.au"]:
                user_type = "sales"
            else:
                user_type = "other"
            self.custom_logger.info(
                f"User type for email address: '{email_id}' is '{user_type}'"
            )
            return user_type
        except Exception as e:
            raise RuntimeError(
                f"Error finding user type for an email : '{email_id}'"
            ) from e

    def __get_email_message(self, user_type, rule_name):
        """
        Private Function to get email message based on user typr
        """
        try:
            if not (email_msgs[user_type].get(rule_name) is None):
                self.custom_logger.info(
                    f"email message found: '{email_msgs[user_type][rule_name]}'"
                )
                return email_msgs[user_type][rule_name]
            else:
                self.custom_logger.info(
                    f"Error finding email message for user type: '{user_type}', rule name :'{rule_name}'"
                )
                return None
        except Exception as e:
            raise RuntimeError(
                f"Error finding email message for user type: '{user_type}', rule name :'{rule_name}'"
            ) from e

    def get_current_year_last_saturday(self):
        """
        Function to get last saturday of the year
        """
        ssm_resp = self.get_ssm_parameter(os.environ["SPLIT_OPP_THRESHOLD_DATE"])
        if (
            type(ssm_resp) is not str
            and ssm_resp.response["Error"]["Code"] == "ParameterNotFound"
        ):
            year = date.today().year
            month = monthcalendar(year, 12)
            # checking if the last week of
            # the month has a saturday
            if month[-1][5]:
                return str(year) + "-12-" + str(month[-1][-2])
            # else print the saturday of the
            # second last week of the month
            else:
                return str(year) + "-12-" + str(month[-2][-2])
        else:
            return ssm_resp

    def sanitize_date_format(self, wc_date):
        """
        Function to get date string in satitized date obj format
        """
        dateTimeObj = datetime.strptime(wc_date, "%Y%m%d")
        wc_date = dateTimeObj.date()
        wc_date = wc_date.strftime("%Y-%m-%d")
        return wc_date

    def create_file_in_s3(self, lines, bucket_name, file_key):
        """
        Function to create file in specified S3 bucket
        """
        self.custom_logger.info(
            f"File creation successful in s3 Bucket: {bucket_name}, File name: {file_key} "
        )
        s3 = boto3.client("s3")
        data = "\n".join(lines)
        s3.put_object(Body=data, Bucket=bucket_name, Key=file_key)
