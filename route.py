import json
import logging
from log import UplightLogger
import os
from base_action import BaseAction
from utils import Event, get_google_oauth_token
from dataclasses import dataclass
from manifest_service import ManifestService
from manifest import ManifestType
from external_manifest import ExternalManifestRequest, ExternalManifest
from gcs import GCS
from manifest import Manifest

logger = UplightLogger("Route")

GCS_FILE_READ_EVENTS = ['OBJECT_FINALIZE']

class Route:
    """
    Route
    Load route configuration from ENVIRONMENT
    Map each route with respective action based on environment variable type
    Trigger the route based on event received
    """

    def __init__(self,APP_ENV: str):
        """
        Initiate configuration of all Routes
            route configuration reads from environment
        Map route with respective action
        """
        logger.event_name = self.__class__.__name__
        logger.event_step = "Init"
        self.__routes = []
        self.__app_env = APP_ENV   
        self.__load_manifest_routes()
        self.is_processed = False
        self.app_env = APP_ENV
        logger.log(f"{len(self.__routes)} Routes Loaded")

    def __load_manifest_routes(self):
        self.__log_step = "load_routes"
        manifest_filename = 'manifest'+'_'+self.__app_env+'_'+'route.json'
        with open(manifest_filename, "r") as fp:
            self.mf_routes = json.load(fp)
        #self.logger.log("Manifest routes loaded")
        for route in self.mf_routes:
                route.update({
                    "events": GCS_FILE_READ_EVENTS
                })
                
                self.__routes.append(ManifestService(**route))

    def trigger(self, request: Event):
        """
        Identify the route based on request and process it
        :param request: Event type request object
        """
        logger.event_step = "trigger"
        logger.log("Finding Route", level=logging.DEBUG)
        for route in self.__routes:
            is_pattern_matched = route.can_handle(request)
            if is_pattern_matched:
                req, temp_file, bucket = route.process(request, is_pattern_matched=is_pattern_matched)
                logger.log("Route Processed")
                self.is_processed = True
        try:
            target_mf_file = Manifest.generate_manifest_filename(req.manifest_file_location,req.mf_type,req.tenant_name)
            logger.log(f"Manifest get generated at -> {target_mf_file}")
            gcs = GCS(oauth_token=get_google_oauth_token())
            #gcs.rename_file(self.bucket, temp_file, target_mf_file.split('/')[-1])
            gcs.write_file_to_gcs(bucket, temp_file, target_mf_file.split('/')[-1])
        except:
            logger.log("No Route matched.")