# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/01_datalake.ipynb.

# %% auto 0
__all__ = ['get_data_catalog']

# %% ../nbs/01_datalake.ipynb 2
import boto3
import json
import yaml
from pathlib import Path
from io import BytesIO
import zipfile
import pandas as pd
import datetime

# %% ../nbs/01_datalake.ipynb 4
class S3ObjectNotFound(Exception):
    pass


class LakeConfigNotFound(Exception):
    pass


class LakeInterface():

    def __init__(
        self,
        config
    ):
        if type(config) == str:
            # load from local config file
            try:
                conf = yaml.safe_load(Path(f'{Path.home()}/.bankdata_config.yml').read_text())
                config_name = config
                config = conf['lake_interface'].get(config_name)
                if config is None:
                    raise LakeConfigNotFound(f'config {config_name} not in Lake config')
            except Exception as e:
                raise LakeConfigNotFound(e)

        session = boto3.session.Session(profile_name=config.get('profile', 'default'))
        self.session=session
        self.s3 = session.client('s3')
        self.athena = session.client('athena')
        self.queries = []
        self.bucket = config.get('default_bucket')
        self.query_results_location = f's3://{self.bucket}/{config.get("query_results_location", "athena_results")}'
        self.athena_workgroup = config.get('athena_workgroup', 'primary')


    def get_object(self, key, bucket=None):
        try:
            return self.s3.get_object(Bucket=self.bucket or DEFAULT_BUCKET, Key=key)
        except Exception as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise S3ObjectNotFound('No S3 object with key = %s' % key)
            else:
                raise

    def load_csv(self,key,bucket=None, delimiter=',', skiprows=None, line_terminator=None):
        obj = self.get_object(key, bucket=bucket or self.bucket)
        if line_terminator:
            return pd.read_csv(obj['Body'], delimiter=delimiter, skiprows=skiprows, lineterminator=line_terminator)
        else:
            return pd.read_csv(obj['Body'], delimiter=delimiter, skiprows=skiprows)
    
    
    def load_json(self, key, output_format='json'):
        obj = self.get_object(key)
        return json.loads(obj['Body'].read())
        
    
    def list_objects(self, prefix, bucket=None):
        
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket or self.bucket, Prefix=prefix)

        return sum([[obj['Key'] for obj in page['Contents']] for page in pages], [])
    

    def save_json(self, path, data, bucket=None, timestamp=None):
        if timestamp:
            key = f'{path}/timestamp={timestamp}/data.json'
        else:
            key = f'{path}/data.json'

        return self.put_object(key, json.dumps(data), bucket=bucket)
        
    def put_object(self, key, data, metadata={}, bucket=None):
        try:
            resp = self.s3.put_object(
                Bucket=bucket or self.bucket,
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

            
    def put(self, path, df, bucket=None, timestamp=None):
        if timestamp:
            key = f'{path}/timestamp={timestamp}/data.parquet'
        else:
            key = f'{path}/data.parquet'

        out_buffer = BytesIO()
        df.to_parquet(
            out_buffer,
            index=True,
            engine='pyarrow',
            compression='gzip',
            allow_truncated_timestamps=True
        )
        if self.put_object(key, out_buffer.getvalue(), bucket=bucket):
            return f'Saved to {key}'
        else:
            return f'Unknown error in save_parquet: {key}'
    
    def most_recent(self, prefix, bucket=None):
        matched_objects = self.list_objects(
            bucket=bucket or self.bucket,
            prefix=prefix
        )
        
        if len(matched_objects) > 1:
            print(f'Multiple objects found for prefix {prefix}')
            return None
        elif len(matched_objects) == 0:
            print(f'No objects found for prefix {prefix}')
            return None
        else:
            return matched_objects[0]

    
    def get(self, path, bucket=None):
        try:
            key = self.most_recent(path, bucket=bucket)
        except Exception as e:
            print(f'No objects found with path: {key}. {e}')
            return None

        resp = self.get_object(key, bucket=bucket)
        return pd.read_parquet(BytesIO(resp['Body'].read()))
                
    def start_query(self, query_def, query_id):
        
        response = self.athena.start_query_execution(
            QueryString=query_def,
            QueryExecutionContext={
                'Database': 'bankdata'
            },
            WorkGroup=self.athena_workgroup,
            ResultConfiguration={"OutputLocation": self.query_results_location}
        )
        
        query_status = response['ResponseMetadata'].get('HTTPStatusCode')
        
        if query_status == 200:
            query_record = {
                'query_def': query_def,
                'query_id': query_id,
                'execution_id': response['QueryExecutionId']
            }
            try:
                query_ids = [r['query_id'] for r in self.queries]
                query_index = query_ids.index(query_id)
                self.queries[query_index] = query_record
            except:
                # query_id is for new query
                self.queries.append(query_record)
                
            response = {
                'status': 'Query Started',
                'execution_id': response['QueryExecutionId'],
                'query_id': query_id
            }
        else:
            response = response['ResponseMetadata']
            response['status'] = 'Query Failed'
        
        return response

    
    def get_query_results(self, query_id):
        query_result_gen = (
            q for q in self.queries
            if q['query_id'] == query_id
        )

        query = next(query_result_gen)
        
        if query:
            results_paginator = self.athena.get_paginator('get_query_results')
            results_iter = results_paginator.paginate(
                QueryExecutionId=query.get('execution_id'),
                PaginationConfig={
                    'PageSize': 1000
                }
            )
            
            data = []
            for rslt_page in results_iter:
                page_data = [[e.get('VarCharValue') for e in row['Data']] for row in rslt_page['ResultSet']['Rows']]
                data.append(page_data)
            
            
            return pd.DataFrame(columns=data[0], data=data[1:])
        else:
            return None



# %% ../nbs/01_datalake.ipynb 12
def unzip(lake_interface, source_file, destination_folder, exclude_pattern=None, include_pattern=None):
    logs = [
        '-' * 30,
        f'Copying from {source_file} to {destination_folder}'
    ]
    
        
    zip_obj = lake_interface.get_object(source_file)
    buffer = BytesIO(zip_obj["Body"].read())

    z = zipfile.ZipFile(buffer)
    file_names = z.namelist()
    filtered_files = file_names.copy()

    if include_pattern:
        filtered_files = [f for f in filtered_files if include_pattern in f]

    if exclude_pattern:
        filtered_files = [f for f in filtered_files if exclude_pattern not in f]

    for filename in filtered_files:
        file_info = z.getinfo(filename)

        lake_interface.s3.upload_fileobj(
            Fileobj=z.open(filename),
            Bucket=lake_interface.bucket,
            Key=f'{destination_folder}/{filename}'
        )

        logs.append(f'Copied {filename}')

    return logs


# %% ../nbs/01_datalake.ipynb 14
def parse_column_info(cols):
    return [
        {
            'column_name': c['Name'], 
            'column_type': c['Type']
        } for c in cols if '__index_level' not in c['Name']
    ]
    
def parse_table_info(raw_table):
    return {
        'table_name': raw_table['Name'],
        'databse_name': raw_table['DatabaseName'],
        'location': raw_table['StorageDescriptor']['Location'],
        'columns': parse_column_info(raw_table['StorageDescriptor']['Columns'])
    }

# %% ../nbs/01_datalake.ipynb 15
def get_data_catalog(lake_interface, database_name):
    """
    Fetches catalog for glue database

    Parameters
    ----------
    lake_interface: LakeInterface, required
        instance of class providing access to datalake
    database_name : str, required
        Name of AWS Glue database

    Returns
    -------
    table_columns : list(str)
        List of all columns in every table in form [table_name].[column_name]

    """
    #harvest aws crawler metadata

    session = lake_interface.session
    glue = session.client('glue')

    next_token = ""
    #glue = boto3.client('glue',region_name='us-east-1')
    tables = []

    while True:
        resp = glue.get_tables(DatabaseName=database_name, NextToken=next_token)

        for tbl in resp['TableList']:
            tables.append(parse_table_info(tbl))
        next_token = resp.get('NextToken')
        
        if next_token is None:
            break

    return tables

