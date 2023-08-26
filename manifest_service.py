#
# manifest_service.py
# Oct 14, 2021
#
# Copyright © Uplight. 2010-2021 All right reserved.
# The copyright to the computer program(s) herein is the property of Uplight.
# The program(s) may be used and/or copied only with the written permission of Uplight.
#
# Manifest Service
# @version 1.0
# @author Sathish Tenkani
# since Oct 14, 2021
#
from base_action import BaseAction
import os
import logging
from utils import Event,get_google_oauth_token
from external_manifest import ExternalManifestRequest, ExternalManifest
from gcs import GCS
from manifest import Manifest

class ManifestService(BaseAction):

    def __init__(self, tenant_id: str, entity: str, **kwargs):
        self.log_module = "ManifestService::MFCreator"
        super().__init__(**kwargs)
        self.logger.event_step = self.__class__.__name__
        self.logger.event_name = "Init"
        self.__tenant_id = tenant_id
        self.__entity = entity

        # required properties for External Manifest service
        self.__uis_spec_conforming = kwargs.get("uis_spec_conforming", False)
        self.__uis_version = kwargs.get("uis_version","3.0.1")
        self.__file_type = kwargs.get("file_type", None)
        self.__delimiter = kwargs.get("delimiter", None)
        self.__encryption_algorithm = kwargs.get("encryption_algorithm", None)
        self.__compression_algorithm = kwargs.get("compression_algorithm", None)
        self.__tenant_name = kwargs.get("tenant_name",None)

        # required properties for Internal Manifest service
        self.__entity_relation = kwargs.get("entity_relation", [])
        self.__destination_location = kwargs.get("destination_location", None)
        self.__destination_mf_file = kwargs.get("mf_key_pattern", "manifest.json")
        self.__src_mf_loc = kwargs.get("source_mf_loc", None)



    def __get_destination_path(self, file_name) -> str:
        """
        In case destination not defined in route config, populate from source itself
        With the assumption of decrypted file can place at source location itself

        Populate destination path for decrypted file
        :param request: Event type request object
        :return str:
        """

        self.logger.event_step = "set_destination"
        if not self.__destination_location:
            target_loc = "/".join(file_name.split("/")[:-1])
            self.__destination_location = os.path.join(f"gs://{self.bucket}", target_loc)

            self.logger.log("Destination Location generated")

        self.logger.log(f"Target location populated - {self.__destination_location}")
        return self.__destination_location

    def __get_external_params(self):
        return {
            "encryption_algorithm": self.__encryption_algorithm,
            "compression_algorithm": self.__compression_algorithm,
        }

    def __get_internal_params(self):
        return {
            "tenant_id": self.__tenant_id,
            "entity_relation": self.__entity_relation,
            "source_manifest_location": self.__src_mf_loc
        }

    def process(self, request: Event, **input_args):
        """
        Process the route as per Event triggered
        :param request: Event type request object
        :param input_args: if any extra params are required
        :return str: success message and status code
        """
        self.logger.event_step = "call_manifest_service"

        for i, file_name in enumerate(request.files):
            if input_args["is_pattern_matched"][i]:
                source = f"gs://{self.bucket}/{file_name}"
                self.logger.log(f"Processing the file.....{i}-->>{source}")
                request_data = {
                    "oauth_token":get_google_oauth_token(),
                    "source_file_fullpath":source,
                    "entity_type":self.__entity,
                    "tenant_name":self.__tenant_name,
                    "delimiter":self.__delimiter,
                    "file_type":self.__file_type,
                    "manifest_file_location":self.__get_destination_path(file_name),
                    "uis_spec_conforming":self.__uis_spec_conforming,
                    "uis_version":self.__uis_version,
                    "mf_type":"external"
                }
                request_data.update(self.__get_external_params())
                mf = None
                req = ExternalManifestRequest(**request_data)
                self.logger.log("Request object for EXTERNAL MANIFEST created successfully, initiating process")
                mf = ExternalManifest(req)
                if mf:
                    self.logger.log("Processing the request...")
                    temp_file = mf.process(i)
                    self.logger.log("Request has been processed successfully")
                else:
                    self.logger.log(f"Unable to process Request")
                    
        # target_mf_file = Manifest.generate_manifest_filename(req.manifest_file_location,req.mf_type,req.tenant_name)
        # self.logger.log(f"Manifest get generated at -> {target_mf_file}")
        # gcs = GCS(oauth_token=get_google_oauth_token())
        # #gcs.rename_file(self.bucket, temp_file, target_mf_file.split('/')[-1])
        # gcs.write_file_to_gcs(self.bucket, temp_file, target_mf_file.split('/')[-1])
        return req, temp_file, self.bucket
