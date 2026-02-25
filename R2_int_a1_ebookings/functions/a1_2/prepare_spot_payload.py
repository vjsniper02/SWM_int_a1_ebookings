from datetime import datetime, timedelta
import os
import logging
import boto3
import json
import math

from functions.a1_2.SalesAreaMap import SalesAreaMap

BUSINESS_TYPE_CODE = "PDS"
BOOKING_TYPE = 2


def lambda_handler(event, context):
    """
    This is the handler to prepare the EBooking Spot Prebooking payload.
    It transforms from BRQ file to the payload to Landmark with additional lookup from SalesArea mapping CSV file.
    Since the payload is big, after this handler, the payload is stored in S3 bucket and `event` object has an
    additional property `spotPayloadFilePath` which will be in format `s3://{bucket_name}/{file_key}`.
    """
    handler = PrepareSpotsPayloadHandler(event)
    filePath = handler.handle()
    event["spotPayloadFilePath"] = filePath

    # return event to pass to next step in the state machine
    return event


class PrepareSpotsPayloadHandler:
    def __init__(self, event):
        self.event = event
        self.correlation_id = event["id"]
        self.opportunity_id = event["detail"]["sf_payload"]["sf"]["opportunityID"]
        self.campaign_code = event["detail"]["campaign_code"]

        self.in_bucket_name = os.environ["EBOOKINGS_S3_FILEIN_BUCKET"]
        self.temp_bucket_name = os.environ["EBOOKINGS_S3_TEMP_BUCKET"]
        self.seil_config_bucket_name = os.environ["SEIL_CONFIG_BUCKET_NAME"]
        self.sales_area_mapping_path = os.environ["SEIL_SALES_AREA_MAPPING_PATH"]
        self.region = os.environ["SEIL_AWS_REGION"]

        self.brq_json_bucket = event["brqJsonBucket"]
        self.brq_json_key = event["brqJsonKey"]

        # Prepare the Sales Area Dict
        self.sales_area_map = SalesAreaMap(sales_area_path=self.sales_area_mapping_path)
        self.sa_dict = {}  # key is the salesAreaNumber
        for area in self.sales_area_map.data:
            self.sa_dict[area["BCC"]] = area

        # prepare the logger with correlation ID
        self.logger = logging.getLogger("prepare_spot_payload.lambda_handler")
        self.logger.__format__ = logging.Formatter(
            self.correlation_id + " - %(message)s"
        )

        self.logger.info("The input event of the prepare_spot_payload.lambda_handler")
        self.logger.info(self.event)

    def handle(self):
        self.__read_brq_json()
        self.__prepare_spot_payload()
        file_path = self.__save_to_s3()
        return file_path
    
    def get_path_by_param_name(self, param_name):
        client = boto3.client("ssm")
        response = client.get_parameter(Name=param_name, WithDecryption=True)

        if "Parameter" not in response or response["Parameter"] == None:
            raise Exception(
                f"Parameter does not exist, The parameter '{self._param_name}' must exist in AWS Parameter Store."
            )

        return response["Parameter"]["Value"]
    
    def __read_brq_json(self):
        s3client = boto3.client("s3", region_name=self.region)
        s3object = s3client.get_object(
            Bucket=self.brq_json_bucket, Key=self.brq_json_key
        )
        file_content = s3object["Body"].read()
        brq_object = json.loads(file_content.decode("utf-8"))

        self.brq_object = brq_object

        return brq_object

    def __lookup_sales_area(self, detail_row):
        station_id = detail_row["StationId"]
        if station_id not in self.sa_dict:
            raise Exception(f"StationID '{station_id}' not found in SalesArea Mapping.")
        else:
            return self.sa_dict[station_id]["code"]

    def __lookup_break_area(self, detail_row):
        station_id = detail_row["StationId"]
        if station_id not in self.sa_dict:
            raise Exception(f"StationID '{station_id}' not found in SalesArea Mapping.")
        else:
            return self.sa_dict[station_id]["breakCode"]

    def __lookup_extra_date(self, detail_row):
        requested_day = detail_row["RequestedDay"]
        # get the first Y
        i = -1
        for char in requested_day:
            i += 1
            if char == "Y":
                break

        wc_date = datetime.strptime(detail_row["WCDate"], "%Y%m%d")
        # result = wc_date + timedelta(days=i)
        result = wc_date
        return result.strftime("%Y-%m-%d")

    def __lineNumber(self, row, index):
        try:
            intresult = int(row["UniqueNetworkProposedSpotId"])
            return intresult
        except:
            return index + 1

    def __requested_day_value(self, requested_day):
        requested_day_list = list(requested_day.strip())
        for index, item in enumerate(requested_day_list):
            if item == "Y":
                return index

    def __prepare_spot_payload(self):
        header = self.brq_object["header"]
        details = self.brq_object["details"]
        spot_payload_details = []

        # ####
        # Please refer to the ticket: https://code7plus.atlassian.net/browse/R2DEV-539
        # for the mapping
        # ####
        multiparts_skip = 0
        one_spot_json = {}
        current_date = datetime.now().date()
        line_count = 1
        for index, row in enumerate(details):
            # Commenting below condition due to UAT defects - 1940, 1932
            # if (
            #     datetime.strptime(self.__lookup_extra_date(row), "%Y-%m-%d").date()
            #     >= current_date
            # ):
            if multiparts_skip > 0:
                multiparts_skip -= 1
                continue  # skip the MD and TA records
            requested_day_value = self.__requested_day_value(row["RequestedDay"])
            scheduled_date = datetime.strptime(row["WCDate"], "%Y%m%d")
            scheduled_date = scheduled_date + timedelta(days=requested_day_value)
            sales_area_code = self.__lookup_sales_area(row)
            break_area_code = self.__lookup_break_area(row)
            one_spot_json = {
                "campaignNumber": self.campaign_code,
                "lineNumber": line_count,
                "versionNumber": 1,
                "spotSalesAreaCode": sales_area_code,
                "breakSalesAreaCode": break_area_code,
                "scheduledDate": scheduled_date.strftime("%Y-%m-%d"),
                "slotStartTime": row["RequestedTime"][0:2]
                + ":"
                + row["RequestedTime"][2:4]
                + ":00",
                "slotEndTime": row["RequestedTime"][4:6]
                + ":"
                + row["RequestedTime"][6:8]
                + ":00",
                "length": row["RequestedSize"],
                "businessTypeCode": BUSINESS_TYPE_CODE,
                # "breakTypeCode": "",
                "bookingType": BOOKING_TYPE,
                "extraFloatData1": row["DemographicOneTarp"],
                "extraFloatData2": row["RequestedGrossRate"],
                "extraStringData1": row["RequestedProgram"],
                "extraStringData2": row["RequestedDay"],
                "extraDateData1": self.__lookup_extra_date(row),
                # "multiparts": [
                #     {
                #         "length": 0,
                #         "extraFloatData2": 0.0
                #     }
                # ]
            }

            multiparts_skip = self.__handle_multiparts(index, details, one_spot_json)
            spot_payload_details.append(one_spot_json)
            line_count += 1

        self._spot_full_payload = {
            "dateTimeStamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "spotPreBookingDetails": spot_payload_details,
        }

    def __handle_multiparts(self, index, details, one_spot_json):
        detail = details[index]

        if "BookingModifiers" not in detail or "TP" not in detail["BookingModifiers"]:
            return 0  # return the number of records to be skipped for the multiparts

        multiparts_skip = 0
        next_index = index
        while True:
            next_index += 1

            if next_index >= len(details):
                return multiparts_skip  # the end of details

            next_detail = details[next_index]

            if "BookingModifiers" in next_detail and (
                "MD" in next_detail["BookingModifiers"]
                or "TA" in next_detail["BookingModifiers"]
            ):
                multiparts_skip += 1

                multipart = {
                    "length": next_detail["RequestedSize"],
                    "extraFloatData2": next_detail["RequestedGrossRate"],
                }

                if "multiparts" not in one_spot_json:
                    one_spot_json["multiparts"] = []
                one_spot_json["multiparts"].append(multipart)

            else:
                return multiparts_skip

    @property
    def spot_full_payload(self):
        return self._spot_full_payload

    def __save_to_s3(self) -> str:
        s3client = boto3.client("s3", region_name=self.region)

        # Extract details
        date_time_stamp = self._spot_full_payload.get("dateTimeStamp")
        details = self._spot_full_payload.get("spotPreBookingDetails", [])

        # Chunk size
        chunk_size = int(self.get_path_by_param_name(os.environ["SPOT_HANDLING_LIMIT"]))
        
        
        total_objects = len(details)

        # If objects <= 5000 → single file in root
        if total_objects <= chunk_size:
            self.event.update(
                {
                    "total_spots": total_objects,
                    "tranche": False,
                }
            )
            filename = f"{self.correlation_id}/spots_payload.json"
            payload_chunk = {
                "dateTimeStamp": date_time_stamp,
                "spotPreBookingDetails": details,
            }
            s3client.put_object(
                Body=json.dumps(payload_chunk),
                Bucket=self.temp_bucket_name,
                Key=filename,
            )
            return f"s3://{self.temp_bucket_name}/{filename}"

        # Else → split into chunks and store in tranche/
        folder_prefix = f"{self.correlation_id}/tranche/"
        num_files = math.ceil(total_objects / chunk_size)
        s3_paths = []

        self.event.update(
            {
                "total_spots": total_objects,
                "tranche": True,
            }
        )

        for i in range(num_files):
            start = i * chunk_size
            end = start + chunk_size
            chunk = details[start:end]

            payload_chunk = {
                "dateTimeStamp": date_time_stamp,
                "spotPreBookingDetails": chunk,
            }

            filename = f"{folder_prefix}spots_payload_{i+1}.json"
            s3client.put_object(
                Body=json.dumps(payload_chunk),
                Bucket=self.temp_bucket_name,
                Key=filename,
            )
            s3_paths.append(f"s3://{self.temp_bucket_name}/{filename}")

        return s3_paths

    # def __save_to_s3(self) -> str:
    #     s3client = boto3.client("s3", region_name=self.region)
    #     filename = self.correlation_id + "/spots_payload.json"
    #     s3client.put_object(
    #         Body=json.dumps(self._spot_full_payload),
    #         Bucket=self.temp_bucket_name,
    #         Key=filename,
    #     )

    #     return f"s3://{self.temp_bucket_name}/{filename}"
