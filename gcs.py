#
# gcs.py
# Jan 17, 2022
#
# Copyright © Uplight. 2010-2022 All right reserved.
# The copyright to the computer program(s) herein is the property of Uplight.
# The program(s) may be used and/or copied only with the written permission of Uplight.
#
# gcs
# @version 1.0
# @author Sathish Tenkani
# since Jan 17, 2022
#
import json
import logging
import os
import re
from datetime import datetime, timedelta

from google.cloud import storage
from google.oauth2.credentials import Credentials
import base64

from log import UplightLogger
from utils import extract_bucket_from_path
import hashlib


class GCS:
    execution_time = ""
    def __init__(self, oauth_token):
        self.client = storage.Client(credentials=Credentials(oauth_token))

        self.logger = UplightLogger("Manifest::GCS")
        self.logger.event_step = self.__class__.__name__
        self.logger.event_name = "Init"

    def is_file_exist(self, file_path: str) -> (str, bool):
        self.logger.eventStep = "is_file_exist"
        self.logger.log(f"Get bucket & folder location, based on given path({file_path})")
        bucket_name, prefix = GCS.get_bucket_and_prefix(file_path)
        self.logger.log(f"Extracted Bucket={bucket_name}, Folder(Prefix)={prefix}")

        try:
            bucket = self.client.bucket(bucket_name)
            self.logger.log(f"Connected to Bucket {bucket_name}")
            stats = storage.Blob(bucket=bucket, name=prefix).exists(self.client)
            self.logger.log(f"Verified file existence {file_path}")
            if not stats:
                self.logger.log(f"File({file_path}) not found")
                return f"File({file_path}) not found", False
            self.logger.log(f"File({file_path}) found")
            return f"File({file_path}) found", True
        except Exception as err:
            self.logger.log(f"Exception encountered during file existence verification",
                            level=logging.ERROR, exc_info=err)
            return f"Exception while accessing file({file_path}) - {str(err)}", False

    def is_manifest_file_exist(self, file_path: str) -> (str, bool):
        self.logger.eventStep = "is_manifest_file_exist"
        self.logger.log(f"Get bucket & folder location, based on given path({file_path})")
        bucket_name, prefix_var = GCS.get_bucket_and_prefix(file_path)
        self.logger.log(f"Extracted Bucket={bucket_name}, Folder(Prefix)={prefix_var}")
        #manifest_folder = "/".join(prefix.split("/")[:-1])
        manifest_location = file_path

        try:
            bucket = self.client.bucket(bucket_name)
            self.logger.log(f"Connected to Bucket {bucket_name}")
            #stats = storage.Blob(bucket=bucket, name=prefix).exists(self.client)
            self.logger.log(f"Getting list of all files present in {prefix_var}")
            blobs = list(bucket.list_blobs(prefix=prefix_var))
            stats = False
            for i_blob in blobs:
                filename = i_blob.name.split("/")[-1:]
                if "manifest" in str(filename).lower() and (GCS.execution_time == str(filename).split('external_manifest_')[-1].split('.')[0]):
                        stats = True
                        self.logger.log(f"File({i_blob.name}) found")
                        return os.path.join(f"gs://{bucket_name}", f"{i_blob.name}"), True
                        break
                else:
                    stats = False

            self.logger.log(f"Verified file existence {file_path}")
            if not stats:
                # self.logger.log(f"no manifest File is present at ({file_path})")
                return f"no manifest File is present at ({file_path})", False
            self.logger.log(f"Verified file existence {file_path}")
        except Exception as err:
            self.logger.log(f"Exception encountered during file existence verification",
                            level=logging.ERROR, exc_info=err)
            return f"Exception while accessing file({file_path}) - {str(err)}", False


    def is_temp_manifest_file_exist(self, file_path: str) -> (str, bool):
        self.logger.eventStep = "is_temp_manifest_file_exist"
        dir_list = os.listdir("/tmp/")
        self.logger.log(f"Getting list of files in current directory({dir_list})")
        try:
            stats = False
            for file in dir_list:
                if "temp" in str(file).lower() and (GCS.execution_time == str(file).split('temp_')[-1].split('.')[0]):
                    stats = True
                    self.logger.log(f"File({file}) found")
                    return f"{file}", True
                    break

            self.logger.log(f"Verified file existence {file_path}")
            if not stats:
                self.logger.log(f"no manifest File is present at ({file_path})")
                return f"no manifest File is present at ({file_path})", False
            self.logger.log(f"Verified file existence {file_path}")
        except Exception as err:
            self.logger.log(f"Exception encountered during file existence verification",
                            level=logging.ERROR, exc_info=err)
            return f"Exception while accessing file({file_path}) - {str(err)}", False
    

    def read_json(self, file_name: str):
        try:
            self.logger.eventStep = "read_json"
            self.logger.log("Read JSON file Content")

            self.logger.log(f"Read JSON data from the file {file_name}")
            data = []
            filename="/tmp/"+file_name
            read_file = open(filename,"r")
            for i in read_file:
                data.append(i)
            data_to_str = ''.join(data)
            data_to_json = json.loads(data_to_str)
            return data_to_json
        except Exception as err:
            self.logger.log(f"Exception encountered while reading JSON file data ({filename})",
                            level=logging.ERROR, exc_info=err)
            raise err


    def write_json(self, filename: str, data: any) -> bool:
        self.logger.eventStep = "write_json"
        self.logger.log("write json data to GCS")
        try:

            file_name = "/tmp/"+filename
            f = open(file_name,"w")
            data_json = json.dumps(data)
            f.write(data_json)
            self.logger.log(f"Successfully writen data to JSON({file_name})")
            return True
        except Exception as err:
            self.logger.log(f"Exception encountered while writing data to JSON file({file_name})",
                            level=logging.ERROR, exc_info=err)
            raise err   

    def get_file_names(self, source_loc, map_file_names):
        self.logger.eventStep = "write_json"
        self.logger.log("Searching for matched file pattern")
        try:
            bucket_name, prefix = GCS.get_bucket_and_prefix(source_loc)
            self.logger.log(f"Extracted Bucket={bucket_name}, Folder(Prefix)={prefix} "
                            f"from the file path {source_loc}")

            gcs_files = self.client.list_blobs(bucket_name, prefix=prefix)
            self.logger.log(f"Listed the objects at {source_loc}")

            matched_files = []

            for blob in gcs_files:
                f_name = blob.name.replace(f"{prefix}/", "")
                for file_name in map_file_names:
                    if re.match(file_name, f_name):
                        matched_files.append(f_name)

            self.logger.log(f"Total {len(matched_files)} file(s) matched with given pattern "
                            f"at {source_loc}")

            return matched_files
        except Exception as err:
            self.logger.log(f"Exception encountered while filtering the files", level=logging.ERROR, exc_info=err)
            raise err
    
    def get_latest_files(self, bucket, last_run_time, file_patterns):
        self.logger.eventStep = "get_latest_files"
        self.logger.log(f"Searching for latest files under {bucket}")
        try:
            blobs = self.client.list_blobs(bucket)
            
            latest_files = []
            for blob in blobs:
                if file_patterns:
                    is_file_matches_pattern = [re.match(pattern, blob.name.split('/')[-1]) for pattern in file_patterns]
                    if any(is_file_matches_pattern):
                        if blob.updated.replace(tzinfo=None) > last_run_time:
                            latest_files.append(blob.name)
                else:
                    if blob.updated.replace(tzinfo=None) > last_run_time:
                            latest_files.append(blob.name)

            self.logger.log(f"Total {len(latest_files)} latest file(s) found as {latest_files}")
            return latest_files
        except Exception as err:
            self.logger.log(f"Exception encountered while searching the latest files", level=logging.ERROR, exc_info=err)
            raise err

    def get_file_metadata(self, file_path):
        self.logger.eventStep = "get_file_metadata"
        self.logger.log("Get file metadata from GCS")
        try:
            bucket_name, prefix = GCS.get_bucket_and_prefix(file_path)
            self.logger.log(f"Extracted Bucket={bucket_name}, Folder(Prefix)={prefix} "
                            f"from the file path {file_path}")
            bucket = self.client.get_bucket(bucket_name)
            self.logger.log(f"Connected to the bucket({bucket_name})")
            file_name = file_path.split("/")[-1]
            self.logger.log(f"Looking for a file({prefix})")
            blob = bucket.get_blob(prefix)
            self.logger.log("Read file metadata from GCS Instance")
            return {
                #"file_name": os.path.join(f"gs://{blob.bucket.name}", f"{blob.name}"),
                "file_name":f"{blob.name}",
                "file_size":blob.size,
                "md5_checksum":base64.b64decode(blob.md5_hash).hex(), #extract md5 checksum from blob metadata & convert into hexadecimal value
                "as_of_datetime":blob.updated
            }
        except Exception as err:
            self.logger.log(f"Exception encountered while reading metadata for {file_path}", level=logging.ERROR,
                            exc_info=err)
            raise err

    def write_file_to_gcs(self, bucket_name, temp_file, new_name):
        """Writing Manifest data to external manifest at gcs."""
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"
        # The ID of the GCS object to rename
        # blob_name = "your-object-name"
        # The new ID of the GCS object
        # new_name = "new-object-name"

        #storage_client = storage.Client()
        bucket = self.client.get_bucket(bucket_name)
        self.logger.log(f"Connected bucket {bucket_name}")
        blob = bucket.blob(new_name)
        data=[]
        temp_filename = "/tmp/"+temp_file
        read_file = open(temp_filename,"r")
        for i in read_file:
            data.append(i)
        data_to_str = ''.join(data)
        blob.upload_from_string(
            data=data_to_str,
            content_type="application/json"
        )
        #new_blob = bucket.rename_blob(blob, new_name)
        print(f"Blob {temp_filename} has been copied to {blob.name}")
        if os.path.exists(temp_filename):
            print("Removing the file", temp_filename)
            os.remove(temp_filename)
        else:
            print("The file does not exist")


    @staticmethod
    def get_bucket_and_prefix(full_path):
        bucket_name = extract_bucket_from_path(full_path)
        prefix = full_path.replace(f"gs://{bucket_name}/", "")
        return bucket_name, prefix

