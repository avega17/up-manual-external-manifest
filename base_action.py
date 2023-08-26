#
# base_action.py
# Aug 17, 2021
#
# Copyright © Uplight. 2010-2021 All right reserved.
# The copyright to the computer program(s) herein is the property of Uplight.
# The program(s) may be used and/or copied only with the written permission of Uplight.
#
# base_action
# @version 1.0
# @author Sathish Tenkani
# since Aug 17, 2021
#

from abc import ABC, abstractmethod
import re
from dataclasses import dataclass
from log import UplightLogger
from utils import Event

class BaseAction(ABC):
    """
    BaseAction

    It's an parent class for all Actions
    Which will manage route configuration details
    And Identification of route for the request, to process
    """
    log_module: str = "BaseAction"

    def __init__(self, name: str, bucket: str,
                 events: list, key_pattern: str, **input_args):
        """
        Common Route parameters across all action classes
        :param name: source file path
        :param bucket: source bucket
        :param events: Supported GCS bucket events
        :param key_pattern: expected file pattern
        :param input_args: if any extra params requires (for future usage)
        """
        self.name = name
        self.bucket = bucket
        self.events = events
        self.key_pattern = key_pattern
        self.logger = UplightLogger(self.log_module)

    def can_handle(self, request: Event):
        """
        Verifies, whether the request was able to handle or not
        :param request: Event type request object
        :return True/ False:
        """
        is_pattern_matched = [re.match(self.key_pattern, f) for f in request.files]
        if request.bucket == self.bucket \
                and any(is_pattern_matched):
            self.__log_route("Triggered Event matched with the following Route ==> ")
            return is_pattern_matched
        self.__log_route("Route has been skipped ==> ")
        return False

    def __log_route(self, log_msg: str):
        """
        Log Route match / skipped along with route config
        :param log_msg:
        :return None:
        """
        self.logger.log(log_msg, name=self.name, bucket=self.bucket,
                        events=self.events, key_pattern=self.key_pattern)

    @abstractmethod
    def process(self, request: Event, **input_args):
        """
        Respective action class should have definition to process route
        :param request: Event type request object
        :param input_args: if any extra params requires (for future usage)
        """
        pass
