

class S3ObjectNotFound(Exception):
    pass


class S3Object():

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
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise S3ObjectNotFound('No S3 object with key = %s' % self.path)
            else:
                raise
    
    def read_object(self):
        pass
