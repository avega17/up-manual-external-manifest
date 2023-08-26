#
# manifest.py
# Jan 17, 2022
#
# Copyright © Uplight. 2010-2022 All right reserved.
# The copyright to the computer program(s) herein is the property of Uplight.
# The program(s) may be used and/or copied only with the written permission of Uplight.
#
# manifest
# @version 1.0
# @author Sathish Tenkani
# since Jan 17, 2022
#
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import os

from gcs import GCS
from log import UplightLogger


class ManifestType(Enum):
    EXTERNAL = 1
    INTERNAL = 2


@dataclass
class ManifestBaseModel:
    oauth_token: str
    uis_spec_conforming: str
    uis_version: str
    entity_type: str
    tenant_name: str
    mf_type: str
    source_file_fullpath: str
    delimiter: str
    manifest_file_location: str


class Manifest(ABC):
    log_module: str = "Manifest"

    def __init__(self, oauth_token: str, **kwargs):
        self.version = os.getenv("SEMANTIC_VERSION", "1.0.0")
        #self.uis_version = os.getenv("uis_version",None)
        self.gcs = GCS(oauth_token)
        self.logger = UplightLogger(self.log_module)
        self.file_name = None

    @classmethod
    def generate_manifest_filename(cls, file_location,mf_type,tenant_name):
        execution_time = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
        GCS.execution_time = execution_time
        # cls.logger.eventStep = "generate_manifest_filename"
        __manifest_file = os.path.join(f"{file_location}", f"{tenant_name}""_"f"{mf_type}""_""manifest_"f"{execution_time}"".json")
        return __manifest_file
    
    def generate_temp_manifest_filename(self, file_location,mf_type,tenant_name):
        execution_time = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
        GCS.execution_time = execution_time
        self.logger.eventStep = "generate_temp_manifest_filename"
        __manifest_file = os.path.join(f"{file_location}", f"{tenant_name}_{mf_type}_temp_{execution_time}.json")
        self.logger.log(f"Temp Manifest get generated at -> {__manifest_file}")
        return __manifest_file

    def update_file_entry(self, mf_info: any, current_file_meta):
        self.logger.eventStep = "update_file_entry"
        __is_file_entry_found = False
        self.logger.log("Searching for current file entry in existing manifest")
        for _file in mf_info['files']:
            if _file['filepath'] == current_file_meta['filepath']:
                self.logger.log(f"{_file['filepath']} matched with current file ({current_file_meta['filepath']})")
                _file.update(current_file_meta)
                __is_file_entry_found = True
                self.logger.log("Updated existing entry with latest info in the Manifest")
                break
        if not __is_file_entry_found:
            self.logger.log("Not found an entry for current file in existing Manifest, "
                            f"hence making an try for {self.file_name}")
            mf_info['files'].append(current_file_meta)

    @abstractmethod
    def manifest_schema(self):
        raise NotImplementedError("manifest_schema needs to be implemented")

    @abstractmethod
    def get_schema_object_attributes(self):
        raise NotImplementedError("manifest_schema needs to be implemented")

    @abstractmethod
    def process(self):
        raise NotImplementedError("manifest_schema needs to be implemented")
