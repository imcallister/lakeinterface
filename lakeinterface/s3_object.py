from abc import ABC, abstractmethod
from lakeinterface.exceptions import S3ObjectNotFound

class S3Object(ABC):
    def __init__(self, s3_session, bucket, path, file_system=None, kwargs={}):
        self.session = s3_session
        self.bucket = bucket
        self.path = path
        self.fs = file_system
        self.kwargs = kwargs
    
    def fetch_object(self):
        try:
            resp = self.session.get_object(Bucket=self.bucket, Key=self.path)
            return resp['Body']
        except Exception as e:
            if hasattr(e, 'response') and e.response['Error']['Code'] == 'NoSuchKey':
                raise S3ObjectNotFound(f'No S3 object with key = {self.path}')
            raise
    
    def fetch_metadata(self):
        resp = self.session.head_object(Bucket=self.bucket, Key=self.path)
        return resp['Metadata']
    
    def update_metadata(self, metadata_updates, overwrite=False):
        
        if overwrite:
            metadata = metadata_updates
        else:
            metadata = self.fetch_metadata()
            if metadata:
                metadata.update(metadata_updates)
            else:
                metadata = metadata_updates
                
        resp = self.session.copy_object(
            Bucket=self.bucket,
            CopySource={'Bucket': self.bucket, 'Key': self.path},
            Key=self.path,
            Metadata=metadata,
            MetadataDirective='REPLACE'
        )
        return resp['ResponseMetadata']['HTTPStatusCode']
        
    @abstractmethod
    def read_object(self):
        """Read and parse the S3 object content"""
        pass

    @abstractmethod 
    def save_object(self, key, obj, metadata=None):
        """Save object to S3"""
        pass
