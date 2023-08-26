#
# log.py
# Jan 19, 2021
#
# Copyright © Uplight. 2010-2022 All right reserved.
# The copyright to the computer program(s) herein is the property of Uplight.
# The program(s) may be used and/or copied only with the written permission of Uplight.
#
# log
# @version 1.0
# @author Sathish Tenkani
# since Jan 19, 2022
#

# from .config import APP_ENV
from google.cloud.logging import Client as GCPLoggingClient
import logging
from utils import to_camel_case

logger = logging.getLogger("CF-GCP-MANIFEST")


class UplightLogger:
    """
    Logger to format all log messages in Uplight Format
    """

    def __init__(self, module, **kwargs):
        """
        Initiate Logger with Uplight format
        :param module: Source module / class / file / component
        :param kwargs: eatra params for future purpose
        """
        self.__event_module = module
        self.event_name = self.__class__.__name__
        self.event_step = "Init"

    @staticmethod
    def enable_gcp_logging():
        try:
            """Attaches GCP logging handler for non-local development"""
            client = GCPLoggingClient()
            client.setup_logging()
            UplightLogger("Logger").log("Google Cloud Logging enabled")
        except Exception as err:
            UplightLogger("Logger").log(f"Exception while enabling GCP Logging {str(err)}")

    def log(self, msg: str, level=logging.INFO, exc_info=None, **log_params):
        """
        Logger Log
            Args:
                msg (str): Messages to log, formatted key=value
                level (int): Log level (ERROR/ INFO/ DEBUG/ WARNING..)
                exc_info: Stack trace for exceptions
            Returns:
                None
        """
        log_msg = f'eventComponent=CLOUD_FUNCTION, eventChannel=DS_MANIFEST_CREATE, ' \
                  f'eventModule={self.__event_module}, eventName={self.event_name}, ' \
                  f'eventStep={self.event_step}'
        for k, v in log_params.items():
            camel_case_key = to_camel_case(k)
            log_msg += ', %s=%s' % (camel_case_key, v)
        log_msg += f", msg={msg}"

        logger.log(level, log_msg, exc_info=exc_info)
