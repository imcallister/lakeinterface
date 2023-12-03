

import boto3
import datetime
import polars as pl
import s3fs
import json
from dateutil.parser import parse

from io import BytesIO

from lakeinterface.config import lake_config


def most_recent(keys, prefix):
    file_types = {o.split('/')[-1] for o in keys}
    
    if len(file_types) > 1:
        raise Exception(f"Mixed filetypes found with prefix {prefix}: {','.join(file_types)}")
    else:
        file_type = next(iter(file_types))

    dates = [
        o.replace(prefix, '').replace(file_type, '').replace('/', '')
        for o in keys
    ]

    timestamp_lengths = {len(d) for d in dates}
    
    if len({len(d) for d in dates}) > 1:
        raise Exception(f'Mixed timestamp formats found with prefix {prefix}')
    else:
        latest = max(dates)
    
    return f'{prefix}/{latest}/{file_type}'


def is_timestamp(ts):
    try:
        return type(parse(ts)) == datetime.datetime
    except:
        return False


DEFAULT_REGION = 'us-east-1'

class S3ObjectNotFound(Exception):
    pass


class Datalake(object):
    """
    A class to wrap interface to an AWS S3 datalake
    ...

    Attributes
    ----------
    session: a boto3 session
    s3 : a boto3 S3 client
    bucket : S3 bucket location of lake
    
    Methods
    -------
    __init__(config_name, aws_profile='default'):
        Initializes the AWS S3 client using AWS profile_name and dict of parameters stored in AWS Systems Manager
    
    get_object(key):
        Core method for loading objects using boto3 S3 client
    
    load_csv(key, delimiter=',', skiprows=None, line_terminator=None):
        Loads csv object with S3 prefix = key
    
    load_json(key):
        Loads json object with S3 prefix = key
        
    list_objects(prefix):
        Lists all objects with S3 prefix = key
    
    save_json(path, data, timestamp=None):
        Saves json object to specified path with an optional timestamp that will be inserted into path
    
    put_object(key, data, metadata={}):
        Core method for saving objects using boto3 S3 client
    
    most_recent(prefix):
        For a given S3 prefix returns object has most recent timestamp
        
    most_recent_folder(prefix):
        For a given S3 prefix returns folder that has most recent timestamp
     
    put(path, df, timestamp=None):
        Saves a dataframe as parquet to specified path with an optional timestamp that will be inserted into path
        
    upload(file_obj, destination_folder, filename, timestamp=None):
        Function for saving general file formats to specified destination in S3 with an optional timestamp that will be inserted into path
    
    get(path, ):
        Loads parquet object from specified path as a dataframe

    """
    
    def __init__(self, config_name, aws_profile=None):
        if aws_profile:
            self.session = boto3.session.Session(profile_name=aws_profile)
        else:
            self.session = boto3.session.Session(region_name=DEFAULT_REGION)
        
        config = lake_config(config_name, aws_profile=aws_profile)
        self.bucket = config.get('bucket')
        self.s3 = self.session.client('s3')
        self.fs = s3fs.S3FileSystem(profile=aws_profile)
        
    def get_object(self, key):
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=key)
            return resp['Body']
        except Exception as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise S3ObjectNotFound('No S3 object with key = %s' % key)
            else:
                raise

    def load_csv(self,key, separator=',', skiprows=None, line_terminator=None):
        obj = self.get_object(key)
        if line_terminator:
            return pl.read_csv(obj, separator=separator, lineterminator=line_terminator)
        else:
            return pl.read_csv(obj, separator=separator)
    
    
    def load_json(self, key):
        obj = self.get_object(key)
        return json.loads(obj.read())
    
    def load_parquet(self, key):
        obj = self.get_object(key)
        return pl.read_parquet(BytesIO(obj.read()))
    
    def get(self, path, not_found_value=None):
        key = self.most_recent(path)
        if key is None:
            if not_found_value:
                return not_found_value
            else:
                raise S3ObjectNotFound('No S3 object at path = %s' % path)

        file_type = key.split('/')[-1]
        if file_type not in ['data.parquet', 'data.json']:
            raise Exception(f'.get not implemented for files of type {file_type}')
        
        if file_type == 'data.parquet':
            return self.load_parquet(key)
        elif file_type == 'data.json':
            return self.load_json(key)
    
    
    def list_objects(self, prefix):
        
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        return sum([[obj['Key'] for obj in page.get('Contents',[]) if obj['Size']>0] for page in pages], [])
    

    def save_json(self, path, data, timestamp=None):
        if timestamp:
            key = f'{path}/{timestamp}/data.json'
        else:
            key = f'{path}/data.json'

        return self.put_object(key, json.dumps(data))
        
    def put_object(self, key, data, metadata={}):
        try:
            resp = self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data
            )
            status_code = resp['ResponseMetadata']['HTTPStatusCode']
            if status_code == 200:
                return True
            else:
                raise Exception(f'Unknown error. Status code: {status_code}')
        except Exception as e:
            raise Exception(f'Unknown error in put object for {key}. {str(e)}')

    
            
    def put(self, path, df, timestamp=None):
        if timestamp:
            key = f'{path}/{timestamp}/data.parquet'
        else:
            key = f'{path}/data.parquet'

        with self.fs.open(f'{self.bucket}/{key}', mode='wb') as f:
            df.write_parquet(f)
        
        return
    
    def upload(self, file_obj, destination_folder, filename, timestamp=None):
        if timestamp:
            key = f'{destination_folder}/{timestamp}/{filename}'
        else:
            key = f'{destination_folder}/{filename}'
            
        return self.s3.upload_fileobj(
            Fileobj=file_obj,
            Bucket=self.bucket,
            Key=key
        )
    
    def most_recent(self, prefix):
        matched_objects = self.list_objects(prefix=prefix)
        
        if len(matched_objects) > 1:
            try:
                return most_recent(matched_objects, prefix)
            except:
                print(f'Multiple objects found for prefix {prefix}. Unable to find most recent.')
                return None
        elif len(matched_objects) == 0:
            return None
        else:
            return matched_objects[0]

    
    def most_recent_folder(self, folder):
        if folder[-1] != '/':
            folder += '/'
            
        timestamps = [i.replace(folder, '').split('/')[0] for i in self.list_objects(folder)]
        if all(is_timestamp(ts) for ts in timestamps):
            return max(timestamps)
        else:
            print(f'Names of child folders do not all end with timestamp. Folder name = {folder}')
            return

