#
# external_manifest.py
# Jan 19, 2022
#
# Copyright © Uplight. 2010-2022 All right reserved.
# The copyright to the computer program(s) herein is the property of Uplight.
# The program(s) may be used and/or copied only with the written permission of Uplight.
#
# external manifest
# @version 1.0
# @author Sathish Tenkani
# since Jan 19, 2022
#
from dataclasses import dataclass
from datetime import datetime
import logging
import hashlib

from manifest import Manifest, ManifestBaseModel


@dataclass
class ExternalManifestRequest(ManifestBaseModel):
    """
    Class for keeping track of an item in inventory.
    Request Event
    """

    encryption_algorithm: str
    compression_algorithm: str
    file_type: str


class ExternalManifest(Manifest):

    def __init__(self, req: ExternalManifestRequest, **kwargs):
        self.log_module = "Manifest::ExternalManifest"
        super().__init__(oauth_token=req.oauth_token, **kwargs)
        self.logger.event_step = self.__class__.__name__
        self.logger.event_name = "Init"
        self.__req = req
        self.file_name = self.__req.source_file_fullpath
        self.uis_spec_conforming = self.__req.uis_spec_conforming

    def manifest_schema(self):
        self.logger.eventStep = "manifest_schema"
        self.logger.log("Populating External Manifest Schema")
        if not self.uis_spec_conforming:
            return {
            "metadata": {"manifest_version":self.version},
            "files":[]
            }
        else:
            return {
                "metadata": {"manifest_version": self.version,
                             "uis_version": self.__req.uis_version
                             },
                "files": []
            }

    def get_schema_object_attributes(self):
        self.logger.eventStep = "get_schema_object_attributes"
        self.logger.log(f"Populate Metadata for Source File")

        __file_metadata = self.gcs.get_file_metadata(self.file_name)
        self.logger.log(f"Fetched Metadata for {self.file_name}")

        self.logger.log(f"populating required attributes")
        file_schema = {"filepath": "/".join(self.file_name.split("/")[3:]),
                       "as_of_datetime": __file_metadata['as_of_datetime'].strftime("%Y-%m-%d %H:%M:%S")}

        if not self.__req.file_type.lower() == 'parquet':
            file_config = dict(file_type=self.__req.file_type, delimiter=self.__req.delimiter)
        else:
            file_config = dict(file_type= self.__req.file_type)
        file_schema["file_config"] = file_config
        file_schema["md5_checksum"] = __file_metadata['md5_checksum']
        file_schema["encryption_algorithm"] = self.__req.encryption_algorithm
        file_schema["compression_algorithm"] = self.__req.compression_algorithm
        if self.uis_spec_conforming:
            file_schema['entity_type'] =self.__req.entity_type

        return file_schema        

    def process(self, i):
        try:
            self.logger.eventStep = "process"
            self.logger.log("Generate External Manifest file")
            msg, file_found = self.gcs.is_file_exist(self.file_name)
            self.logger.log(msg)
            if not file_found:
                self.logger.log("There might be some issue to read source file"
                                f" {self.file_name}", level=logging.ERROR)
                raise FileNotFoundError(msg)

            msg,manifest_file_found = self.gcs.is_temp_manifest_file_exist(self.__req.manifest_file_location)
            if manifest_file_found:
                self.logger.log("Manifest already exist, hence reading content to update")
                temp_mf_file = msg
                mf_data = self.gcs.read_json(temp_mf_file)
                # print("mf_data is ",mf_data)
            else:
                self.logger.log(f"Creating new Manifest")
                temp_mf_file = self.generate_temp_manifest_filename(self.__req.manifest_file_location,self.__req.mf_type,self.__req.tenant_name)
                temp_mf_file = str(temp_mf_file.split('/')[-1])
                self.logger.log(f"External Manifest get generated at {temp_mf_file}")
                mf_data = self.manifest_schema()

            _current_file_metadata = self.get_schema_object_attributes()
            self.update_file_entry(mf_data, _current_file_metadata)

            self.gcs.write_json(temp_mf_file, mf_data)
            self.logger.log(f"Updated Meta schema for {self.file_name}")
            return temp_mf_file
        except Exception as err:
            self.logger.log("Exception encountered while creating External Manifest",
                            level=logging.ERROR, exc_info=err)
            raise err

