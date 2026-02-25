import io
import os
from zipfile import ZipFile

from functions.BRQParser import BRQParser


class TestBRQParser:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_parse_header(self):
        brq_file_name = "brq_test_files/simple.brq"
        with open(
            os.path.join(os.path.dirname(__file__), brq_file_name), "r"
        ) as brq_file:
            brq_file_content = brq_file.read()
        parser = BRQParser(brq_file_content)

        # parse the whole BRQ file
        parser.parse()
        assert parser.has_error() == False

        header_object = parser.get_header()

        assert header_object["GenerationDate"] == "20231219"
        assert header_object["GenerationTime"] == "1052"
        assert header_object["NetworkId"] == "SEVNET"
        assert header_object["NetworkName"] == "Seven Network"
        assert header_object["AgencyId"] == "B00017"
        assert header_object["AgencyName"] == "R2SIT_SPARK FOUNDRY"
        assert header_object["BookingDetailRecordCounter"] == 0
        assert header_object["BookingTotalGrossValue"] == float(0)
        assert header_object["ProposedDetailRecordCounter"] == 6
        assert header_object["ProposedTotalGrossValue"] == float(375)
        assert header_object["NarrativeRecordCounter"] == 0
        assert header_object["NetworkDomainName"] == ""
        assert header_object["NetworkContactName"] == "Simon Templar"
        assert (
            header_object["NetworkContactEmail"] == "simon.templar@sevenaffiliatesale"
        )
        assert header_object["AgencyDomainName"] == ""
        assert header_object["AgencyContactName"] == "Bill Flower"
        assert header_object["AgencyContactEmail"] == "bill.kecskes@cognizant.com"

    def test_narrative_objet(self):
        brq_file_name = "brq_test_files/narrative_test.brq"
        with open(
            os.path.join(os.path.dirname(__file__), brq_file_name), "r"
        ) as brq_file:
            brq_file_content = brq_file.read()

        parser = BRQParser(brq_file_content)

        # parse the whole BRQ file
        parser.parse()
        assert parser.has_error() == False

        header_object = parser.get_header()
        narrative_lines = parser.get_narratives()

        assert header_object["NarrativeRecordCounter"] == 5
        assert narrative_lines[0] == "Line1"
        assert narrative_lines[1] == "Line2"
        assert narrative_lines[2] == "Line3"
        assert narrative_lines[3] == "Line4"
        assert narrative_lines[4] == "Line5"

    def test_details(self):
        brq_file_name = "brq_test_files/simple.brq"
        with open(
            os.path.join(os.path.dirname(__file__), brq_file_name), "r"
        ) as brq_file:
            brq_file_content = brq_file.read()
        parser = BRQParser(brq_file_content)

        # parse the whole BRQ file
        parser.parse()
        assert parser.has_error() == False

        header_object = parser.get_header()
        narrative_lines = parser.get_narratives()
        details = parser.get_details()

        assert len(details) == header_object["ProposedDetailRecordCounter"]

        assert details[0]["ClientId"] == "A00040"
        assert details[0]["ClientName"] == "R2SIT_WESTPAC BANKIN"
        assert details[0]["ClientProductId"] == "61"
        assert details[0]["ClientProductName"] == "Activation"
        assert details[0]["StationId"] == "TS38"
        assert details[0]["StationName"] == "7 Wagga"
        assert details[0]["UniqueNetworkProposedSpotId"] == ""
        assert details[0]["UniqueNetworkPreviousSpotId"] == ""
        assert details[0]["UniqueNetworkParentSpotId"] == ""
        assert details[0]["UniqueAgencyProposedSpotId"] == "0000028244-000000001"
        assert details[0]["UniqueAgencyPreviousSpotId"] == ""
        assert details[0]["UniqueAgencyParentSpotId"] == "0000028244-000000001"
        assert details[0]["WCDate"] == "20240107"
        assert details[0]["ProposedDay"] == ""
        assert details[0]["ProposedStartEndTime"] == ""
        assert details[0]["RequestedDay"] == "NNNNNYN"
        assert details[0]["RequestedTime"] == "06000900"
        assert details[0]["ProposedSize"] == 0
        assert details[0]["ProposedGrossRate"] == 0.0
        assert details[0]["ProposedNetRate"] == 0.0
        assert details[0]["RequestedSize"] == 15
        assert details[0]["RequestedGrossRate"] == 69.0
        assert details[0]["RequestedNetRate"] == 62.10
        assert details[0]["ProposedProgram"] == ""
        assert details[0]["RequestedProgram"] == "Sunrise"
        assert details[0]["KeyNumber"] == ""
        assert details[0]["MaterialInstruction"] == ""
        assert details[0]["DemographicOneThousand"] == 0.0
        assert details[0]["DemographicTwoThousand"] == 0.0
        assert details[0]["DemographicThreeThousand"] == 0.0
        assert details[0]["DemographicFourThousand"] == 0.0
        assert details[0]["RecordType"] == "PR"
        assert details[0]["RatingsOverrideFlag1"] == ""
        assert details[0]["RatingsOverrideFlag2"] == ""
        assert details[0]["RatingsOverrideFlag3"] == ""
        assert details[0]["RatingsOverrideFlag4"] == ""
        assert details[0]["DemographicCodeOne"] == "002"
        assert details[0]["DemographicOneTarp"] == 1.2
        assert details[0]["DemographicCodeTwo"] == ""
        assert details[0]["DemographicTwoTarp"] == 0
        assert details[0]["DemographicCodeThree"] == ""
        assert details[0]["DemographicThreeTarp"] == 0
        assert details[0]["DemographicCodeFour"] == ""
        assert details[0]["DemographicFourTarp"] == 0
        assert len(details[0]["BookingModifiers"]) == 0

    def test_details_with_booking_modifies(self):
        brq_file_name = "brq_test_files/booking_modify_test.brq"
        with open(
            os.path.join(os.path.dirname(__file__), brq_file_name), "r"
        ) as brq_file:
            brq_file_content = brq_file.read()
        parser = BRQParser(brq_file_content)

        # parse the whole BRQ file
        parser.parse()
        assert parser.has_error() == False

        header_object = parser.get_header()
        narrative_lines = parser.get_narratives()
        details = parser.get_details()

        assert len(details) == header_object["ProposedDetailRecordCounter"]

        assert all([a == b for a, b in zip(details[0]["BookingModifiers"], ["AB"])])
        assert all(
            [a == b for a, b in zip(details[1]["BookingModifiers"], ["AB", "CD"])]
        )
        assert all(
            [a == b for a, b in zip(details[2]["BookingModifiers"], ["AB", "CD", "EF"])]
        )
        assert len(details[3]["BookingModifiers"]) == 0
        assert len(details[4]["BookingModifiers"]) == 0
        assert len(details[5]["BookingModifiers"]) == 0

    def test_invalid_header_brq(self):
        brq_file_name = "brq_test_files/invalid_header.brq"
        parser = BRQParser(self.__read_brq_file_content(brq_file_name))

        # parse the whole BRQ file
        parser.parse()
        assert parser.has_error() == True
        assert str(parser.get_error()) == "Header line must be 418 character length."

    def test_invalid_eof_brq(self):
        brq_file_name = "brq_test_files/invalid_eof.brq"
        parser = BRQParser(self.__read_brq_file_content(brq_file_name))

        # parse the whole BRQ file
        parser.parse()
        assert parser.has_error() == True
        assert str(parser.get_error()) == "Last line must be 'EOF//'."

    def test_invalid_details_brq(self):
        brq_file_name = "brq_test_files/invalid_details.brq"
        parser = BRQParser(self.__read_brq_file_content(brq_file_name))

        # parse the whole BRQ file
        parser.parse()
        assert parser.has_error() == True
        assert str(parser.get_error()) == "Detail line #1 has less then 620 characters."

    def test_invalid_details_2_brq(self):
        brq_file_name = "brq_test_files/invalid_details_2.brq"
        parser = BRQParser(self.__read_brq_file_content(brq_file_name))

        # parse the whole BRQ file
        parser.parse()
        assert parser.has_error() == True
        assert str(parser.get_error()) == "Detail line #1 is not end with '//'."

    def __read_brq_file_content(self, brq_file_name):
        with open(
            os.path.join(os.path.dirname(__file__), brq_file_name), "r"
        ) as brq_file:
            brq_file_content = brq_file.read()
        return brq_file_content
