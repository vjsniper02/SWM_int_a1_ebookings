import io
import json


def mock_client_generator(type_class_dict):
    return lambda type, region_name="": (
        type_class_dict[type](region_name) if type in type_class_dict else None
    )


def mock_lambda_simple_return(return_value):
    class mock_lambda_client:
        def __init__(region_name):
            pass

        def invoke(self, FunctionName, InvocationType="", Payload=None):
            return {"Payload": io.BytesIO(json.dumps(return_value).encode("utf-8"))}

    return lambda type, region_name="": mock_lambda_client()
