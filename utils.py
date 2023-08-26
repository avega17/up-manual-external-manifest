#
# utils.py
# Jan 17, 2022
#
# Copyright © Uplight. 2010-2022 All right reserved.
# The copyright to the computer program(s) herein is the property of Uplight.
# The program(s) may be used and/or copied only with the written permission of Uplight.
#
# utils
# @version 1.0
# @author Sathish Tenkani
# since Jan 17, 2022
#

from dataclasses import dataclass
from google import auth
from google.auth.transport import requests
import google.oauth2.id_token

def extract_bucket_from_path(path):
    return path.replace("gs://", "").split("/")[0]


def to_camel_case(s):
    """Convert snake case to camel case
    Args:
        s (str): string to convert
    Returns:
        string in camel case
    """
    parts = s.split('_')
    return parts[0] + ''.join(c.title() for c in parts[1:])

def get_google_oauth_token():
    """
    Google default Authentication to get updated refresh token
    :return GOOGLE_OAUTH_TOKEN:
    """
    credentials, project = auth.default()
    auth_req = requests.Request()
    credentials.refresh(auth_req)
    oauth_token = credentials.token
    return oauth_token

@dataclass
class Event:
    """
    Class for keeping track of an item in inventory.
    Request Event
    """
    bucket: str
    files: list
    event_type: str
    batch_id: str = None
