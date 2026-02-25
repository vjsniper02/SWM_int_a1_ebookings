import os
from urllib.parse import urlparse
import re
from swm_logger.swm_common_logger import LambdaLogger

custom_logger = LambdaLogger(log_group_name=os.environ["LOG_GROUP_NAME"])

def resolve_brq_file_name(brq_file_name_url, context, event_id,brq_type=""):
    try:
        custom_logger.info("Resolving BRQ file name", context, correlationId=event_id)
        url = urlparse(brq_file_name_url)
        brq_file_name_with_email = os.path.basename(url.path)
        if brq_type != "CHILD":
            from_email = brq_file_name_with_email.split("_", 1)[0]
            brq_file_name = brq_file_name_with_email.split("_", 1)[1]

            matches = re.search("(.*)-Request-(.*).brq$", brq_file_name, re.IGNORECASE)
            if matches and matches[2]:
                brq_request_id = matches[2]
            else:
                custom_logger.info(f"Invalid BRQ File Name '{brq_file_name_url}'", context, correlationId=event_id)
                raise RuntimeError(f"Invalid BRQ File Name '{brq_file_name_url}'")
        else:
            from_email = ""
            brq_file_name = brq_file_name_url.split("/", 1)[1]
            matches = re.search("(.*)-Request-(.*).brq$", brq_file_name, re.IGNORECASE)
            if matches and matches[2]:
                brq_request_id = matches[2]

        return (from_email, brq_request_id, brq_file_name)
    except:
        custom_logger.info(f"Invalid BRQ File Name '{brq_file_name_url}'", context, correlationId=event_id)
        raise RuntimeError(f"Invalid BRQ File Name '{brq_file_name_url}'")
