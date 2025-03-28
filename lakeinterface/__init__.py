import boto3
import s3fs

from lakeinterface.s3_object_manager import S3ObjectManager


DEFAULT_REGION = 'us-east-1'


class Datalake(object):
    """
    A class to wrap interface to an AWS S3 datalake.

    Two primary file types are supported: parquet and json.  The file type is inferred from the file extension.
    The get and put methods are the core methods for loading and saving objects from/to memory.  
    The assumption is that data transformation is done on either polars dataframes or json objects.

    There are additional utility methods for the onboarding of data in arbitrary file formats into the datalake.

    ...

    Attributes
    ----------
    session: a boto3 session
    s3 : an instance of S3ObjectManager class defined in lakeinterface/s3_object_manager.py
    
    Methods
    -------
    __init__(config_name, aws_profile='default'):
        Initializes the S3ObjectManager using AWS profile_name and dict of parameters stored in AWS Systems Manager
    
    get(path, not_found_value=None):
        Loads parquet from specified path into a dataframe or a json object into the equivalent python object

    put(path, obj, timestamp=None):
        Saves dataframes as parquet and json-like objects to json at the specified path with an optional timestamp that will be inserted into path
    
    list_objects(prefix):
        Lists all objects with S3 prefix = key
    
    most_recent(prefix):
        For a given S3 prefix returns object has most recent timestamp
        
    most_recent_folder(prefix):
        For a given S3 prefix returns folder that has most recent timestamp
    
    def save_file(file_obj, destination_folder, filename, timestamp=None):
        Save a file object of any type to S3

    def load_file(path):
        Open a file object of any type saved in S3
    
    """
    
    def __init__(self, aws_profile=None):
        if aws_profile:
            self.session = boto3.session.Session(profile_name=aws_profile)
        else:
            self.session = boto3.session.Session(region_name=DEFAULT_REGION)
        
        self.obj_mgr = S3ObjectManager(
            self.session, 
            s3fs.S3FileSystem(profile=aws_profile)
        )

    def get(self, path, not_found_value=None):
        return self.obj_mgr.fetch_object(path, not_found_value=not_found_value)

    def get_metadata(self, path):
        return self.obj_mgr.fetch_metadata(path)
    
    def update_metadata(self, path, updates, overwrite=False):
        return self.obj_mgr.update_metadata(path, updates, overwrite=overwrite)

    def list_objects(self, path):
        return self.obj_mgr.list_objects(path)

    def list_versions(self, path):
        return self.obj_mgr.list_versions(path)

    def put(self, path, obj, metadata=None):
        return self.obj_mgr.save_object(path, obj, metadata=metadata)
    
    def copy(self, from_path, to_path, metadata=None):
        return self.obj_mgr.copy(from_path, to_path, metadata=metadata)
    
    def upload_file(self, file_obj, destination_path, metadata={}):
        return self.obj_mgr.upload_file(file_obj, destination_path, metadata=metadata)

    def load_file(self, path):
        return self.obj_mgr.load_file(path)
    
    def read_file(self, path):
        return self.obj_mgr.load_file(path).read().decode('utf-8')
    
    def unzip_to_s3(self, zip_obj, destination_folder):
        return self.obj_mgr.unzip_to_s3(zip_obj, destination_folder)

    # delete these methods?

    def most_recent(self, path):
        return self.obj_mgr.most_recent(path)
    
    def most_recent_folder(self, folder):
        return self.obj_mgr.most_recent_folder(folder)
    
    