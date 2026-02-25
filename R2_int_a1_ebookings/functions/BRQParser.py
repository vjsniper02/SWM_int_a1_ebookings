class BRQParser:
    brq_header_slice_config = {
        "GenerationDate": [0, 8],
        "GenerationTime": [8, 12],
        "NetworkId": [12, 18],
        "NetworkName": [18, 58],
        "AgencyId": [58, 64],
        "AgencyName": [64, 104],
        "BookingDetailRecordCounter": [104, 110, int],
        "BookingTotalGrossValue": [
            110,
            120,
            lambda value: BRQParser.__convert_money(value),
        ],
        "ProposedDetailRecordCounter": [120, 126, int],
        "ProposedTotalGrossValue": [
            126,
            136,
            lambda value: BRQParser.__convert_money(value),
        ],
        "NarrativeRecordCounter": [136, 138, int],
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
        "ProposedGrossRate": [304, 314, lambda value: BRQParser.__convert_money(value)],
        "ProposedNetRate": [314, 324, lambda value: BRQParser.__convert_money(value)],
        "RequestedSize": [324, 332, int],
        "RequestedGrossRate": [
            332,
            342,
            lambda value: BRQParser.__convert_money(value),
        ],
        "RequestedNetRate": [342, 352, lambda value: BRQParser.__convert_money(value)],
        "ProposedProgram": [352, 392],
        "RequestedProgram": [392, 432],
        "KeyNumber": [432, 452],
        "MaterialInstruction": [452, 512],
        "DemographicOneThousand": [512, 519, float],
        "DemographicTwoThousand": [519, 526, float],
        "DemographicThreeThousand": [526, 533, float],
        "DemographicFourThousand": [533, 540, float],
        "RecordType": [540, 542],
        "RatingsOverrideFlag1": [542, 543],
        "RatingsOverrideFlag2": [543, 544],
        "RatingsOverrideFlag3": [544, 545],
        "RatingsOverrideFlag4": [545, 546],
        "DemographicCodeOne": [546, 561],
        "DemographicOneTarp": [561, 564, "decimaltwoandone"],
        "DemographicCodeTwo": [564, 579],
        "DemographicTwoTarp": [579, 582, "decimaltwoandone"],
        "DemographicCodeThree": [582, 597],
        "DemographicThreeTarp": [597, 600, "decimaltwoandone"],
        "DemographicCodeFour": [600, 615],
        "DemographicFourTarp": [615, 618, "decimaltwoandone"],
        "BookingModifiers": [
            618,
            None,
            lambda value: BRQParser.__convert_booking_modifier(value),
        ],
    }

    def __init__(self, brq_file_content: str):
        """
        Create  new Parse BRQ file content to BRQ file object of Python dictionary object with header object and details array.
        :param brq_file_content: A string content that for the whole BRQ file content. It is a string value with multilines separated by \\n.
        """
        self.brq_file_content = brq_file_content
        self.brq_lines = self.brq_file_content.splitlines()
        self.error = None

    def parse(self) -> dict:
        try:
            """
            The return object matches the design of https://code7plus.atlassian.net/wiki/spaces/CODE7/pages/105349138/A1+-+eBooking+Detailed+Design#BRQ-Parser
            After the file parsed successfully, headers, narratives and details will be set to the corresponding values.
            If the file parsed with error, the error object will be set
            """
            self.__check_header()
            self.__check_eof()

            self.__parse_header()
            self.__parse_narratives()
            self.__parse_details()

            self.json = {
                "header": self.header,
                "narrativeRecords": self.narratives,
                "details": self.details,
            }
            return self.json
        except Exception as err:
            self.error = err

    def get_header(self) -> dict:
        return self.header

    def get_narratives(self) -> [str]:
        return self.narratives

    def get_details(self) -> [dict]:
        return self.details

    def has_error(self) -> bool:
        return self.error != None

    def get_error(self) -> Exception:
        return self.error

    def __parse_header(self) -> dict:
        """
        Parse the first line of BRQ file to header object
        :param str first_line: String value representing the first line of BRQ file
        """
        first_line = self.brq_lines[0]
        self.header = self.__parse_one_line(
            first_line, BRQParser.brq_header_slice_config
        )

    def __parse_narratives(self) -> [str]:
        if self.header["NarrativeRecordCounter"] > 0:
            self.narratives = self.brq_lines[
                1 : self.header["NarrativeRecordCounter"] + 1
            ]
        else:
            self.narratives = []

    def __parse_details(self) -> [dict]:
        if self.header["ProposedDetailRecordCounter"] <= 0:
            self.details = []
        narrative_counter = self.header["NarrativeRecordCounter"]
        result = []
        counter = 1
        for oneline in self.brq_lines[(1 + narrative_counter) : -1]:
            self.__check_detail_line(counter, oneline)
            result.append(
                self.__parse_one_line(
                    oneline, BRQParser.brq_detail_records_slice_config
                )
            )
            counter += 1

        self.details = result

    def __parse_one_line(self, oneline: str, config_dict: dict) -> dict:
        result_object = {}
        for field in config_dict:
            config = config_dict[field]
            if config[1] == None:
                result_object[field] = oneline[config[0] :].strip()
            else:
                result_object[field] = oneline[config[0] : config[1]].strip()
            if len(config) >= 3 and config[2]:  # type conversion
                result_object[field] = BRQParser.__convert(
                    result_object[field], config[2]
                )
        return result_object

    def __check_header(self):
        first_line = self.brq_lines[0]
        if len(first_line) != 418:
            raise Exception("Header line must be 418 character length.")

    def __check_eof(self):
        last_line = self.brq_lines[-1]
        if last_line != "EOF//":
            raise Exception("Last line must be 'EOF//'.")

    def __check_detail_line(self, detail_line_number, detail_line):
        if len(detail_line) < 620:
            raise Exception(
                f"Detail line #{detail_line_number} has less then 620 characters."
            )
        if detail_line[-2:] != "//":
            raise Exception(f"Detail line #{detail_line_number} is not end with '//'.")

    @classmethod
    def __convert_booking_modifier(cls, value):
        if value:
            str_value = value[:-2]  # remove the ending //
            n = 2  # group by 2 characters
            return [str_value[i : i + n] for i in range(0, len(str_value), n)]
        else:
            return []

    @classmethod
    def __convert_money(cls, value):
        if value:
            return float(value[0:-2] + "." + value[-2:])
        else:
            return float(0)

    @classmethod
    def __convert(cls, value, value_type):
        try:
            if value_type == "money":
                return float(value[0:-2] + "." + value[-2:])
            if value_type == "decimaltwoandone":  # 231 -> 23.1, 001 -> 0.1
                return int(value) / 10.0
            else:
                return value_type(value)
        except:
            if value_type == "money":
                return float(0)
            else:
                return value_type()
