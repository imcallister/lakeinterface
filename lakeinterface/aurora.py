from lakeinterface.config import lake_config
import json
import psycopg2
import boto3
import polars as pl


class Aurora(object):
    """
    A class to wrap connections to an AWS Aurora postgres cluster.
    Implemented as a singleton to reduce number of open connections
    ...

    Attributes
    ----------
    write_host : endpoint of writer instance of the cluster
    read_host : endpoint of reader instance of the cluster
    secret_id : id of AWS Secrets Manager secret containing connection credentials
    write_conn : pyscopg2 connection to write_host
    read_conn : pyscopg2 connection to read_host
    
    Methods
    -------
    __init__(config, profile='default'):
        Initializes the AWS Aurora connections using AWS profile_name and dict of parameters from ConfigManager
    
    aurora_connection(host):
        Core class for opening connections
    
    open_write_connection():
        Opens a pyscopg2 connection to write_host using aurora_connection method.

    open_read_connection():
        Opens a pyscopg2 connection to read_host using aurora_connection method.
        
    close_connections():
        Closes both read and write connections.
        
    query(sql):
        Executes a query on the read connection

    """
    _instance = None

    def __new__(cls, config, profile_name='default'):
        if cls._instance is None:
            cls._instance = super(Aurora, cls).__new__(cls)
            # Put any initialization here.
        return cls._instance
    
    def __init__(self, config, profile_name=None):
        self.session = boto3.session.Session(profile_name=profile_name)
        
        self.write_host = config['aurora_writedb']
        self.read_host = config['aurora_readdb']
        self.secret_id = config['aurora_secret_id']
        
        self.open_read_connection()
        self.open_write_connection()
        
    def open_read_connection(self):
        self.read_conn = self.aurora_connection(
            self.read_host
        )
        
    def open_write_connection(self):
        self.write_conn = self.aurora_connection(
            self.write_host
        )
        
    def close_connections(self):
        self.write_conn.close()
        self.read_conn.close()
        
    def __exit__(self):
        self.close_connections()
        
    def aurora_connection(self, host):
        sman = self.session.client('secretsmanager')
        secret = sman.get_secret_value(SecretId=self.secret_id)
        credentials = json.loads(secret['SecretString'])

        return psycopg2.connect(
            database="bankdata",
            host=host,
            user=credentials['username'],
            password=credentials['password'],
            port="5432"
        )
    
    def query(self, sql):
        if self.read_conn.closed == 1:
            self.open_read_connection()
            
        with self.read_conn.cursor() as cursor:
            cursor.execute(sql)

            try:
                col_names = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                self.read_conn.commit()
                
                df = pl.DataFrame(data, schema=col_names, orient='row')

                return df
            except Exception as e:
                self.read_conn.commit()
                raise e
