import polars as pl
from dateutil.parser import parse
import datetime
import botocore
from io import BytesIO
import zipfile
import logging

from lakeinterface.parquet_s3_object import ParquetS3Object
from lakeinterface.json_s3_object import JSONS3Object
from lakeinterface.csv_s3_object import CSVS3Object
from lakeinterface.s3_object import S3Object
from lakeinterface.exceptions import UnsupportedFileType, LakeError, S3ObjectNotFound
from lakeinterface.path_handler import S3PathHandler
from lakeinterface.version_manager import S3VersionManager
from lakeinterface.object_operations import S3ObjectOperations


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


def parse_path(path, file_type=None):
    path = path.replace('s3://', '')
    if path[-1] == '/':
        path = path[:-1]

    parts = path.split('/')
    if file_type and parts[-1].endswith(f'.{file_type}') is False:
        parts[-1] += f'.{file_type}'

    if len(parts) == 2:
        return parts[0], parts[1]
    else:
        return parts[0], '/'.join(parts[1:])


def infer_file_type(path, content):
    file_name = path.split('/')[-1]
    if '.' in file_name:
        file_suffix = file_name.split('.')[-1]
        if file_suffix in ['parquet', 'json', 'csv', 'zip']:
            return file_suffix
        else:
            raise Exception(f'File type {file_suffix} not supported')
    elif type(content) == pl.DataFrame:
        return 'parquet'
    else:
        return 'json'


class S3ObjectManager():

    SUPPORTED_FILE_TYPES = {
        'parquet': ParquetS3Object,
        'json': JSONS3Object,
        'csv': CSVS3Object
    }

    def __init__(self, session, file_system):
        self.client = session.client('s3')
        self.fs = file_system
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.path_handler = S3PathHandler()
        self.version_manager = S3VersionManager(self.client)
        self.object_ops = S3ObjectOperations(self.client, self.fs)


    def list_objects(self, path):
        bucket, prefix = parse_path(path)
        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        all_objects = sum([[f"{bucket}/{obj['Key']}" for obj in page.get('Contents',[]) if obj['Size']>0] for page in pages], [])

        # we do not want to include objects that are incomplete matches
        # eg if prefix == foo/bar/group1 then we want to exclude foo/bar/group10
        # but we do want to list incomplete matches where the only difference is the suffix
        return [o for o in all_objects if f'{bucket}/{prefix}/' in o or f'{bucket}/{prefix}' == o or f'{bucket}/{prefix}' == o.replace('.parquet', '').replace('.json', '')]
    

    def list_versions(self, path):
        if self.check_key_exists(path) is False:
            print(f'No S3 object at path = {path}')
            return
        
        bucket, prefix = parse_path(path)
        versions_raw = self.client.list_object_versions(Bucket=bucket, Prefix=prefix)
        versions = [
            {'version_id': v['VersionId'], 'last_modified': v['LastModified'], 'is_latest': v['IsLatest']} 
            for v in versions_raw['Versions']
        ]
        return sorted(versions, key=lambda x: x['last_modified'], reverse=True)


    def check_key_exists(self, path):
        bucket, key = parse_path(path)

        try:
            self.client.head_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            elif e.response['Error']['Code'] == 403:
                return False
            else:
                raise
        return True

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
        obj = S3Object(self.client, bucket, key)
        return obj.fetch_metadata()
    
    def update_metadata(self, path, metadata_updates, overwrite=False):
        key = self.most_recent(path)
        bucket, key = parse_path(key)
        obj = S3Object(self.client, bucket, key)
        return obj.update_metadata(metadata_updates, overwrite=overwrite)

    def fetch_object(self, path, not_found_value=None, **kwargs):
        """Fetch and parse an S3 object"""
        key = self.most_recent(path)
        if key is None:
            if not_found_value is not None:
                return not_found_value
            raise S3ObjectNotFound(f'No S3 object at path = {path}')

        bucket, key = self.path_handler.parse_path(key)
        file_type = key.split('.')[-1]
        
        handler = self._get_object_handler(file_type, bucket, key, **kwargs)
        return handler.read_object()
        
    def save_object(self, path, content, metadata=None):
        file_type = infer_file_type(path, content)
        bucket, key = parse_path(path, file_type)

        match file_type:
            case 'parquet':
                obj = ParquetS3Object(self.client, bucket, key, file_system=self.fs)
            case 'json':
                obj = JSONS3Object(self.client, bucket, key)
            case 'csv':
                obj = CSVS3Object(self.client, bucket, key)
            case 'zip':
                raise Exception('Zip files not supported')
        
        return obj.save_object(key, content, metadata=metadata)
    
    def copy(self, from_path, to_path, metadata=None):
        from_bucket, from_key = parse_path(from_path)
        to_bucket, to_key = parse_path(to_path)
        
        copy_source = {
            'Bucket': from_bucket,
            'Key': from_key
        }
        
        if metadata is None:
            return self.client.copy_object(
                Bucket=to_bucket,
                Key=to_key,
                CopySource=copy_source,
                MetadataDirective='COPY'
            )
        else:
            return self.client.copy_object(
                Bucket=to_bucket,
                Key=to_key,
                CopySource=copy_source,
                Metadata=metadata,
                MetadataDirective='REPLACE'
            )
        

    def upload_file(self, file_obj, destination_path, metadata={}):
        bucket, key = parse_path(destination_path)
        
        return self.client.upload_fileobj(
            Fileobj=file_obj,
            Bucket=bucket,
            Key=key,
            ExtraArgs={"Metadata": metadata}
        )
    
    def load_file(self, path):
        bucket, key = parse_path(path)
        try:
            resp = self.client.get_object(Bucket=bucket, Key=key)
            return resp['Body']
        except Exception as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise S3ObjectNotFound('No S3 object with key = %s' % key)
            else:
                raise

    def unzip_to_s3(self, zip_obj, destination_folder):
        buffer = BytesIO(zip_obj.read())
    
        z = zipfile.ZipFile(buffer)
        file_names = z.namelist()
        filtered_files = file_names.copy()

        for filename in filtered_files:
            #file_info = z.getinfo(filename)
            self.upload_file(
                z.open(filename),
                f'{destination_folder}/{filename}'
            )

    def _get_object_handler(self, file_type, bucket, key, **kwargs):
        """Get appropriate handler for file type"""
        handler_class = self.SUPPORTED_FILE_TYPES.get(file_type)
        if not handler_class:
            raise UnsupportedFileType(f'File type {file_type} not supported')
        return handler_class(self.client, bucket, key, **kwargs)

    def _infer_file_type(self, path, content):
        """Infer the file type from path or content"""
        file_name = path.split('/')[-1]
        if '.' in file_name:
            file_suffix = file_name.split('.')[-1].lower()
            if file_suffix in self.SUPPORTED_FILE_TYPES:
                return file_suffix
            raise UnsupportedFileType(f'File type {file_suffix} not supported')
        
        if isinstance(content, pl.DataFrame):
            return 'parquet'
        return 'json'

    