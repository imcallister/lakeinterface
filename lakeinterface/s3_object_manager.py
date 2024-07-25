import polars as pl
from dateutil.parser import parse
import datetime

from lakeinterface.parquet_s3_object import ParquetS3Object
from lakeinterface.json_s3_object import JSONS3Object
from lakeinterface.csv_s3_object import CSVS3Object
from lakeinterface.s3_object import S3Object


def most_recent(keys, path):
    file_types = {o.split('/')[-1] for o in keys}
    
    if len(file_types) > 1:
        raise Exception(f"Mixed filetypes found with path {path}: {','.join(file_types)}")
    else:
        file_type = next(iter(file_types))

    dates = [
        o.replace(path, '').replace(file_type, '').replace('/', '')
        for o in keys
    ]

    if len({len(d) for d in dates}) > 1:
        raise Exception(f'Mixed timestamp formats found with path {path}')
    else:
        latest = max(dates)
    
    return f'{path}/{latest}/{file_type}'


def is_timestamp(ts):
    try:
        return type(parse(ts)) == datetime.datetime
    except:
        return False


def parse_path(path):
    if path[-1] == '/':
        path = path[:-1]

    parts = path.split('/')
    if len(parts) == 2:
        return parts[0], parts[1]
    else:
        return parts[0], '/'.join(parts[1:])


class S3ObjectNotFound(Exception):
    pass


class S3ObjectManager():

    def __init__(self, session, file_system):
        self.s3 = session.client('s3')
        self.fs = file_system


    def list_objects(self, path):
        bucket, prefix = parse_path(path)
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        return sum([[f"{bucket}/{obj['Key']}" for obj in page.get('Contents',[]) if obj['Size']>0] for page in pages], [])
    

    def most_recent(self, path):
        matched_objects = self.list_objects(path)
        
        if len(matched_objects) > 1:
            try:
                return most_recent(matched_objects, path)
            except:
                print(f'Multiple objects found for path {path}. Unable to find most recent.')
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

    def fetch_metadata(self, path):
        key = self.most_recent(path)
        bucket, key = parse_path(key)
        obj = S3Object(self.s3, bucket, key)
        return obj.fetch_metadata()
    
    def update_metadata(self, path, metadata_updates, overwrite=False):
        key = self.most_recent(path)
        bucket, key = parse_path(key)
        obj = S3Object(self.s3, bucket, key)
        return obj.update_metadata(metadata_updates, overwrite=overwrite)

    def fetch_object(self, path, not_found_value=None, kwargs={}):
        key = self.most_recent(path)
        bucket, key = parse_path(key)
        if key is None:
            if not_found_value:
                return not_found_value
            else:
                raise S3ObjectNotFound('No S3 object at path = %s' % path)

        file_type = key.split('/')[-1].split('.')[-1]

        match file_type:
            case 'parquet':
                obj = ParquetS3Object(self.s3, bucket, key)
            case 'json':
                obj = JSONS3Object(self.s3, bucket, key)
            case _:
                raise Exception(f'Lakeinterface not implemented for files of type {file_type}')

        return obj.read_object()
        
    def save_object(self, path, content, timestamp=None, metadata=None):
        if type(content) == pl.DataFrame:
            file_type = 'parquet'
        else:
            file_type = 'json'

        bucket, prefix = parse_path(path)
        if timestamp:
            key = f'{prefix}/{timestamp}/data.{file_type}'
        else:
            key = f'{prefix}/data.{file_type}'

        match file_type:
            case 'parquet':
                obj = ParquetS3Object(self.s3, bucket, key, file_system=self.fs)
            case 'json':
                obj = JSONS3Object(self.s3, bucket, key)
        
        return obj.save_object(key, content, metadata=metadata)
    
    def upload(self, file_obj, destination_folder, filename, timestamp=None):
        bucket, prefix = parse_path(destination_folder)
        
        if timestamp:
            key = f'{prefix}/{timestamp}/{filename}'
        else:
            key = f'{prefix}/{filename}'
            
        return self.s3.upload_fileobj(
            Fileobj=file_obj,
            Bucket=bucket,
            Key=key
        )
    
    def save_file(self, file_obj, destination_folder, filename, timestamp=None):
        bucket, prefix = parse_path(destination_folder)

        if timestamp:
            key = f'{prefix}/{timestamp}/{filename}'
        else:
            key = f'{prefix}/{filename}'
            
        resp = self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=file_obj
        )
        return resp['ResponseMetadata']['HTTPStatusCode']
    
    def load_file(self, path):
        bucket, key = parse_path(path)
        try:
            resp = self.s3.get_object(Bucket=bucket, Key=key)
            return resp['Body']
        except Exception as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise S3ObjectNotFound('No S3 object with key = %s' % key)
            else:
                raise