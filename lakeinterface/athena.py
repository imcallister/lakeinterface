import boto3

from lakeinterface.config import lake_config


class LakeConfigNotFound(Exception):
    pass


class Athena():

    def __init__(self, profile_name=None):
        
        self.session = boto3.session.Session(profile_name=profile_name)
        self.config = lake_config('bankdata', aws_profile=profile_name)
        
        self.athena = self.session.client('athena')
        self.glue = self.session.client('glue')
        self.queries = []
        self.bucket = self.config.get('bucket')
        self.query_results_location = f's3://{self.bucket}/{self.config.get("query_results_location", "athena_results")}'
        self.athena_workgroup = self.config.get('athena_workgroup', 'primary')
    
                
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

    
    def get_query_status(self, execution_id):
        response = self.athena.get_query_execution(
            QueryExecutionId=execution_id
        )
        return response['QueryExecution']['Status']['State']
        
    def get_query_results(self, query_id):
        query_result_gen = (
            q for q in self.queries
            if q['query_id'] == query_id
        )

        query = next(query_result_gen)
        
        if query:
            status = (self.get_query_status(query['execution_id']))
            if status == 'SUCCEEDED':
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

                #return pd.DataFrame(columns=data[0], data=data[1:])
                return data
            else:
                print(f'Query not complete. Status: {status}')
                return None
        else:
            return None


    def get_data_catalog(self, database_name):
        """
        Fetches catalog for glue database

        Parameters
        ----------
        database_name : str, required
            Name of AWS Glue database

        Returns
        -------
        table_columns : list(str)
            List of all columns in every table in form [table_name].[column_name]

        """
        #harvest aws crawler metadata

        next_token = ""
        #glue = boto3.client('glue',region_name='us-east-1')
        tables = []

        while True:
            resp = self.glue.get_tables(DatabaseName=database_name, NextToken=next_token)

            for tbl in resp['TableList']:
                tables.append(parse_table_info(tbl))
            next_token = resp.get('NextToken')

            if next_token is None:
                break

        return tables


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
        'database_name': raw_table['DatabaseName'],
        'location': raw_table['StorageDescriptor']['Location'],
        'columns': parse_column_info(raw_table['StorageDescriptor']['Columns'])
    }
