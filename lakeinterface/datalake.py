import boto3
import s3fs
from dateutil.parser import parse

from lakeinterface.config import lake_config
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
        Initializes the AWS S3 client using AWS profile_name and dict of parameters stored in AWS Systems Manager
    
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
    
    
        

    get_object(key):
        Core method for loading objects using boto3 S3 client
    
    load_csv(key, delimiter=',', skiprows=None, line_terminator=None):
        Loads csv object with S3 prefix = key
    
    load_json(key):
        Loads json object with S3 prefix = key
        
    
    save_json(path, data, timestamp=None):
        Saves json object to specified path with an optional timestamp that will be inserted into path
    
    put_object(key, data, metadata={}):
        Core method for saving objects using boto3 S3 client
    
     
        
    upload(file_obj, destination_folder, filename, timestamp=None):
        Function for saving general file formats to specified destination in S3 with an optional timestamp that will be inserted into path
    
    
    """
    
    def __init__(self, config_name, aws_profile=None):
        if aws_profile:
            self.session = boto3.session.Session(profile_name=aws_profile)
        else:
            self.session = boto3.session.Session(region_name=DEFAULT_REGION)
        
        config = lake_config(config_name, aws_profile=aws_profile)

        self.s3 = S3ObjectManager(
            self.session, 
            config.get('bucket'), 
            s3fs.S3FileSystem(profile=aws_profile)
        )

    def get(self, path, not_found_value=None):
        return self.s3.fetch_object(path, not_found_value=not_found_value)

    def list_objects(self, prefix):
        return self.s3.list_objects(prefix=prefix)

    def most_recent(self, prefix):
        return self.s3.most_recent(prefix=prefix)
    
    def most_recent_folder(self, folder):
        return self.s3.most_recent_folder(folder)

    def put(self, path, obj, timestamp=None, file_type=None):
        return self.s3.save_object(path, obj, timestamp=timestamp, file_type=file_type)
    
    def upload(self, file_obj, destination_folder, filename, timestamp=None):
        return self.s3.upload(file_obj, destination_folder, filename, timestamp=timestamp)
    
    def save_file(self, file_obj, destination_folder, filename, timestamp=None):
        return self.s3.save_file(file_obj, destination_folder, filename, timestamp=timestamp)

    def load_file(self, path):
        return self.s3.load_file(path)
