import json
import io
import os
import boto3
import re
import csv


class SalesAreaMap:
    """
    SalesAreaMap is the class to read Sales Area Mapping CSV file.
    """

    def __init__(
        self,
        param_name: str = None,
        sales_area_path: str = None,
        sales_area_csv: str = None,
    ):
        """
        Create a new instance of SalesAreaMap.
        :param str param_name: The parameter name in AWS Parmaeter Store
        :param str sales_area_path: The CSV file path for the Sales Area Mapping CSV file. The format must be "s3://bucket-name/path/to/filename".
        :param str sales_area_csv: The Sales Area Mapping CSV file content as a single string.

        One of param_name, sales_area_path, sales_area_csv must be provided.
        sales_area_csv will be considered first if that is provided.
        sales_area_path will be used next if that is provided.
        param_name will be used last if that is provided.
        """
        self._param_name = param_name
        self._sales_area_path = sales_area_path
        self._sales_area_csv = sales_area_csv

        if (
            not self._param_name
            and not self._sales_area_csv
            and not self._sales_area_path
        ):
            raise ValueError(
                "param_name, sales_area_path or sales_area_csv must be provided."
            )

        if self._sales_area_csv:
            self.__content_to_dict()
        elif self._sales_area_path:
            self.__get_file_content_by_path()
            self.__content_to_dict()
        elif self._param_name:
            self.__get_path_by_param_name()
            self.__get_file_content_by_path()
            self.__content_to_dict()

    @property
    def data(self):
        return self._data

    @property
    def fieldnames(self):
        return self._fieldnames

    def __content_to_dict(self):
        sales_area_dict_reader = csv.DictReader(io.StringIO(self._sales_area_csv))
        self._fieldnames = sales_area_dict_reader.fieldnames
        self._data = list(sales_area_dict_reader)

    def __get_file_content_by_path(self):
        matches = re.search("s3:\/\/([a-zA-Z0-9_\-\.]+)\/(.+)", self._sales_area_path)
        if not matches:
            raise Exception(
                f"Parameter '{self._param_name}' has invalid value '{self._sales_area_path}'. The value must in format \"s3://bucket-name/path/file.csv\"."
            )

        bucket = matches[1]
        key = matches[2]
        client = boto3.client("s3")
        s3object = client.get_object(Bucket=bucket, Key=key)
        self._sales_area_csv = s3object["Body"].read().decode("utf-8-sig")

    def __get_path_by_param_name(self):
        client = boto3.client("ssm")
        parameter = client.get_parameter(Name=self._param_name, WithDecryption=True)

        if "Parameter" not in parameter or parameter["Parameter"] == None:
            raise Exception(
                f"Sales Area Mapping CSV file path parmaeter does not exist. The parameter '{self._param_name}' must exist in AWS Parameter Store."
            )

        self._sales_area_path = parameter["Parameter"].Value
