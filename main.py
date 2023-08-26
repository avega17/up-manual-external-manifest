#
# main.py
# Jan 17, 2022
#
# Copyright © Uplight. 2010-2022 All right reserved.
# The copyright to the computer program(s) herein is the property of Uplight.
# The program(s) may be used and/or copied only with the written permission of Uplight.
#
# main
# @version 1.0
# @author Sathish Tenkani
# since Jan 17, 2022
#

from log import UplightLogger
from manifest import ManifestType
from route import Route
from utils import Event, get_google_oauth_token
from gcs import GCS

from datetime import datetime, timedelta
from croniter import croniter
import json
import base64
import logging
import os
from dataclasses import asdict

logger = UplightLogger("main")

# Entry point
def create_manifest(event, context):
    """Triggered by Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
            {
                '@type':'<type_of_even_triggered_this>',
                'attributes'(dict): 'Notification object',
                'data': '<data_posted_from_source_in_base64_encoded_format>'
            }
         context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        logger.eventName = __name__
        logger.eventStep = "Init"

        # Enable GCP logging for Cloud Environment
        UplightLogger.enable_gcp_logging()
        logger.log("Initiated")
        logger.log("Request received for EXTERNAL MANIFEST file creation")

        pub_sub = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        tenant_name = pub_sub['tenant_name']
        file_patterns = pub_sub.get('file_patterns', None)

        # Get runtime environment variables and create bucket name
        APP_ENV = os.environ.get('env', None)
        APP_SOV = os.environ.get('sov', None)
        source_bucket = 'uplight-'+tenant_name+'-incoming-'+APP_ENV+'-'+APP_SOV
        # logger.log(f"bucket ---> {source_bucket}")
        
        # Get last (tenant specific) function execution time using cron provided
        itr = croniter(pub_sub['cron'], datetime.now().replace(second=0, microsecond=0))
        last_run_time = itr.get_prev(datetime)

        gcs = GCS(oauth_token=get_google_oauth_token())
        latest_files = gcs.get_latest_files(source_bucket, last_run_time, file_patterns)
        
        request_object = Event(bucket=source_bucket, files=latest_files,
                            event_type='OBJECT_FINALIZE',
                            batch_id=pub_sub.get("batch_id", None))

        logger.log("Request ", level=logging.DEBUG, **asdict(request_object))
    
        route = Route(APP_ENV)
        route.trigger(request_object)
        GCS.execution_time = ""
        if route.is_processed:
            logger.log("Route Processed")
            return 200
        else:
            logger.log(f"Route not found: {request_object}, hence skipping the process")
                    
    except Exception as err:
        logger.log(f"Encountered an Error: {str(err)}", level=logging.ERROR, exc_info=err)
        raise err
