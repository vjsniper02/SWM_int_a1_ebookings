import logging
import boto3
import os
import json
import time
from boto3 import client as boto3_client
from datetime import date, datetime, timedelta
from calendar import monthcalendar
import botocore.exceptions
from functions.common_utils import CommonUtils
from swm_logger.swm_common_logger import LambdaLogger

custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])

# brq_json = json.load(open(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'brq_config.json'))))


def lambda_handler(event, context):
    """
    Funtion to validate the Demo Tolerance in BRQ file
    """
    common_utils = CommonUtils(event, custom_logger, context)
    event["brqSplit"] = "NO"
    if event["validationResult"]["continueValidation"] is True:
        custom_logger.info(f"event_data: {event}")
        brq_spots_data_one = []
        brq_spots_data_two = []
        brq_header_data_one = []
        brq_header_data_two = []
        sat_date = common_utils.get_current_year_last_saturday()
        brq_filename = event["brqFileName"].split(".")
        s3 = boto3.resource("s3")
        key = event["brqFileName"] + ".json"
        s3_object = s3.Object(event["brqJsonPath"], key)
        file_content = s3_object.get()["Body"].read().decode("utf-8")
        json_content = json.loads(file_content)
        split_one_record_count = 0
        split_two_record_count = 0
        split_two_proposed_total = 0
        split_one_proposed_total = 0
        for item in json_content["details"]:
            wc_date = common_utils.sanitize_date_format(item["WCDate"])
            wc_date = datetime.strptime(f"{wc_date}", "%Y-%m-%d")
            campaign_end_date = wc_date + timedelta(days=6)
            custom_logger.info(campaign_end_date)
            if campaign_end_date > datetime.strptime(sat_date, "%Y-%m-%d"):
                split_two_proposed_total += item["RequestedGrossRate"]
                split_two_record_count += 1
                brq_spots_data_two.append(
                    item["ClientId"].ljust(6)
                    + item["ClientName"].ljust(40)
                    + item["ClientProductId"].ljust(6)
                    + item["ClientProductName"].ljust(40)
                    + item["StationId"].ljust(6)
                    + item["StationName"].ljust(40)
                    + item["UniqueNetworkProposedSpotId"].ljust(20)
                    + item["UniqueNetworkPreviousSpotId"].ljust(20)
                    + item["UniqueNetworkParentSpotId"].ljust(20)
                    + item["UniqueAgencyProposedSpotId"].ljust(20)
                    + item["UniqueAgencyPreviousSpotId"].ljust(20)
                    + item["UniqueAgencyParentSpotId"].ljust(20)
                    + item["WCDate"].ljust(8)
                    + item["ProposedDay"].ljust(7)
                    + item["ProposedStartEndTime"].ljust(8)
                    + item["RequestedDay"].ljust(7)
                    + str(item["RequestedTime"]).ljust(8)
                    + item["ProposedSize"].ljust(8)
                    + item["ProposedGrossRate"].ljust(10)
                    + item["ProposedNetRate"].ljust(10)
                    + str("{:08d}".format(item["RequestedSize"])).ljust(8)
                    + str(
                        "{:08d}".format(
                            int(str(item["RequestedGrossRate"]).split(".", 1)[0])
                        )
                        + "{:02d}".format(
                            int(str(item["RequestedGrossRate"]).split(".", 1)[1])
                        )
                    ).ljust(10)
                    + str(
                        "{:08d}".format(
                            int(str(item["RequestedNetRate"]).split(".", 1)[0])
                        )
                        + "{:02d}".format(
                            int(str(item["RequestedNetRate"]).split(".", 1)[1])
                        )
                    ).ljust(10)
                    + item["ProposedProgram"].ljust(40)
                    + item["RequestedProgram"].ljust(40)
                    + item["KeyNumber"].ljust(20)
                    + item["MaterialInstruction"].ljust(60)
                    + item["DemographicOneThousand"].ljust(7)
                    + item["DemographicTwoThousand"].ljust(7)
                    + item["DemographicThreeThousand"].ljust(7)
                    + item["DemographicFourThousand"].ljust(7)
                    + item["RecordType"].ljust(2)
                    + item["RatingsOverrideFlag1"].ljust(1)
                    + item["RatingsOverrideFlag2"].ljust(1)
                    + item["RatingsOverrideFlag3"].ljust(1)
                    + item["RatingsOverrideFlag4"].ljust(1)
                    + item["DemographicCodeOne"].ljust(15)
                    + str(
                        "{:02d}".format(
                            int(str(item["DemographicOneTarp"]).split(".", 1)[0])
                        )
                        + "{:01d}".format(
                            int(str(item["DemographicOneTarp"]).split(".", 1)[1])
                        )
                    ).ljust(3)
                    + item["DemographicCodeTwo"].ljust(15)
                    + str(
                        "{:02d}".format(
                            int(str(item["DemographicTwoTarp"]).split(".", 1)[0])
                        )
                        + "{:01d}".format(
                            int(str(item["DemographicTwoTarp"]).split(".", 1)[1])
                        )
                    ).ljust(3)
                    + item["DemographicCodeThree"].ljust(15)
                    + str(
                        "{:02d}".format(
                            int(str(item["DemographicThreeTarp"]).split(".", 1)[0])
                        )
                        + "{:01d}".format(
                            int(str(item["DemographicThreeTarp"]).split(".", 1)[1])
                        )
                    ).ljust(3)
                    + item["DemographicCodeFour"].ljust(15)
                    + str(
                        "{:02d}".format(
                            int(str(item["DemographicFourTarp"]).split(".", 1)[0])
                        )
                        + "{:01d}".format(
                            int(str(item["DemographicFourTarp"]).split(".", 1)[1])
                        )
                    ).ljust(3)
                    + "  //"
                )
            else:
                split_one_proposed_total += item["RequestedGrossRate"]
                split_one_record_count += 1
                brq_spots_data_one.append(
                    item["ClientId"].ljust(6)
                    + item["ClientName"].ljust(40)
                    + item["ClientProductId"].ljust(6)
                    + item["ClientProductName"].ljust(40)
                    + item["StationId"].ljust(6)
                    + item["StationName"].ljust(40)
                    + item["UniqueNetworkProposedSpotId"].ljust(20)
                    + item["UniqueNetworkPreviousSpotId"].ljust(20)
                    + item["UniqueNetworkParentSpotId"].ljust(20)
                    + item["UniqueAgencyProposedSpotId"].ljust(20)
                    + item["UniqueAgencyPreviousSpotId"].ljust(20)
                    + item["UniqueAgencyParentSpotId"].ljust(20)
                    + item["WCDate"].ljust(8)
                    + item["ProposedDay"].ljust(7)
                    + item["ProposedStartEndTime"].ljust(8)
                    + item["RequestedDay"].ljust(7)
                    + str(item["RequestedTime"]).ljust(8)
                    + item["ProposedSize"].ljust(8)
                    + item["ProposedGrossRate"].ljust(10)
                    + item["ProposedNetRate"].ljust(10)
                    + str("{:08d}".format(item["RequestedSize"])).ljust(8)
                    + str(
                        "{:08d}".format(
                            int(str(item["RequestedGrossRate"]).split(".", 1)[0])
                        )
                        + "{:02d}".format(
                            int(str(item["RequestedGrossRate"]).split(".", 1)[1])
                        )
                    ).ljust(10)
                    + str(
                        "{:08d}".format(
                            int(str(item["RequestedNetRate"]).split(".", 1)[0])
                        )
                        + "{:02d}".format(
                            int(str(item["RequestedNetRate"]).split(".", 1)[1])
                        )
                    ).ljust(10)
                    + item["ProposedProgram"].ljust(40)
                    + item["RequestedProgram"].ljust(40)
                    + item["KeyNumber"].ljust(20)
                    + item["MaterialInstruction"].ljust(60)
                    + item["DemographicOneThousand"].ljust(7)
                    + item["DemographicTwoThousand"].ljust(7)
                    + item["DemographicThreeThousand"].ljust(7)
                    + item["DemographicFourThousand"].ljust(7)
                    + item["RecordType"].ljust(2)
                    + item["RatingsOverrideFlag1"].ljust(1)
                    + item["RatingsOverrideFlag2"].ljust(1)
                    + item["RatingsOverrideFlag3"].ljust(1)
                    + item["RatingsOverrideFlag4"].ljust(1)
                    + item["DemographicCodeOne"].ljust(15)
                    + str(
                        "{:02d}".format(
                            int(str(item["DemographicOneTarp"]).split(".", 1)[0])
                        )
                        + "{:01d}".format(
                            int(str(item["DemographicOneTarp"]).split(".", 1)[1])
                        )
                    ).ljust(3)
                    + item["DemographicCodeTwo"].ljust(15)
                    + str(
                        "{:02d}".format(
                            int(str(item["DemographicTwoTarp"]).split(".", 1)[0])
                        )
                        + "{:01d}".format(
                            int(str(item["DemographicTwoTarp"]).split(".", 1)[1])
                        )
                    ).ljust(3)
                    + item["DemographicCodeThree"].ljust(15)
                    + str(
                        "{:02d}".format(
                            int(str(item["DemographicThreeTarp"]).split(".", 1)[0])
                        )
                        + "{:01d}".format(
                            int(str(item["DemographicThreeTarp"]).split(".", 1)[1])
                        )
                    ).ljust(3)
                    + item["DemographicCodeFour"].ljust(15)
                    + str(
                        "{:02d}".format(
                            int(str(item["DemographicFourTarp"]).split(".", 1)[0])
                        )
                        + "{:01d}".format(
                            int(str(item["DemographicFourTarp"]).split(".", 1)[1])
                        )
                    ).ljust(3)
                    + "  //"
                )
        brq_spots_data_one.append("EOF//")
        brq_spots_data_two.append("EOF//")
        if split_two_record_count > 0 and split_one_record_count > 0:
            event["brqSplit"] = "YES"
            custom_logger.info(f"WC dates are greater than threshold, Split Opp = YES")
            # Split BRQ files is being generated only for next two years(Current and next year) based on the discussion/confirmation.
            brq_header_data_one.append(
                str(json_content["header"].get("GenerationDate")).ljust(8)
                + str(json_content["header"].get("GenerationTime")).ljust(4)
                + json_content["header"].get("NetworkId").ljust(6)
                + json_content["header"].get("NetworkName").ljust(40)
                + json_content["header"].get("AgencyId").ljust(6)
                + json_content["header"].get("AgencyName").ljust(40)
                + json_content["header"].get("BookingDetailRecordCounter").ljust(6)
                + json_content["header"].get("BookingTotalGrossValue").ljust(10)
                + str("{:06d}".format(split_one_record_count)).ljust(6)
                + str(
                    "{:08d}".format(int(str(split_one_proposed_total).split(".", 1)[0]))
                    + "{:02d}".format(
                        int(str(split_one_proposed_total).split(".", 1)[1])
                    )
                ).ljust(10)
                + str(
                    "{:02d}".format(
                        json_content["header"].get("NarrativeRecordCounter")
                    )
                ).ljust(2)
                + json_content["header"].get("NetworkDomainName").ljust(40)
                + json_content["header"].get("NetworkContactName").ljust(30)
                + json_content["header"].get("NetworkContactEmail").ljust(70)
                + json_content["header"].get("AgencyDomainName").ljust(40)
                + json_content["header"].get("AgencyContactName").ljust(30)
                + json_content["header"].get("AgencyContactEmail").ljust(70)
            )
            brq_header_data_two.append(
                str(json_content["header"].get("GenerationDate")).ljust(8)
                + str(json_content["header"].get("GenerationTime")).ljust(4)
                + json_content["header"].get("NetworkId").ljust(6)
                + json_content["header"].get("NetworkName").ljust(40)
                + json_content["header"].get("AgencyId").ljust(6)
                + json_content["header"].get("AgencyName").ljust(40)
                + json_content["header"].get("BookingDetailRecordCounter").ljust(6)
                + json_content["header"].get("BookingTotalGrossValue").ljust(10)
                + str("{:06d}".format(split_two_record_count)).ljust(6)
                + str(
                    "{:08d}".format(int(str(split_one_proposed_total).split(".", 1)[0]))
                    + "{:02d}".format(
                        int(str(split_one_proposed_total).split(".", 1)[1])
                    )
                ).ljust(10)
                + str(
                    "{:02d}".format(
                        json_content["header"].get("NarrativeRecordCounter")
                    )
                ).ljust(2)
                + json_content["header"].get("NetworkDomainName").ljust(40)
                + json_content["header"].get("NetworkContactName").ljust(30)
                + json_content["header"].get("NetworkContactEmail").ljust(70)
                + json_content["header"].get("AgencyDomainName").ljust(40)
                + json_content["header"].get("AgencyContactName").ljust(30)
                + json_content["header"].get("AgencyContactEmail").ljust(70)
            )
            brq_split_one_data = [*brq_header_data_one, *brq_spots_data_one]
            brq_split_two_data = [*brq_header_data_two, *brq_spots_data_two]
            common_utils.create_file_in_s3(
                brq_split_one_data,
                os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
                event["brqDirPath"] + "/" + brq_filename[0] + "-1.brq",
            )
            common_utils.create_file_in_s3(
                brq_split_two_data,
                os.environ["EBOOKINGS_S3_TEMP_BUCKET"],
                event["brqDirPath"] + "/" + brq_filename[0] + "-2.brq",
            )
            event["childBrqOnePath"] = (
                event["brqDirPath"] + "/" + brq_filename[0] + "-1.brq"
            )
            event["childBrqTwoPath"] = (
                event["brqDirPath"] + "/" + brq_filename[0] + "-2.brq"
            )
    return event
