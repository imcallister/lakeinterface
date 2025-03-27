from lakeinterface.exceptions import S3ObjectNotFound
from lakeinterface.path_handler import S3PathHandler

class S3VersionManager:
    def __init__(self, s3_client):
        self.client = s3_client
        self.path_handler = S3PathHandler()

    def list_versions(self, path):
        """List all versions of an S3 object"""
        if not self.check_key_exists(path):
            raise S3ObjectNotFound(f'No S3 object at path = {path}')
        
        bucket, prefix = self.path_handler.parse_path(path)
        versions_raw = self.client.list_object_versions(Bucket=bucket, Prefix=prefix)
        
        versions = [
            {
                'version_id': v['VersionId'],
                'last_modified': v['LastModified'],
                'is_latest': v['IsLatest']
            } 
            for v in versions_raw['Versions']
        ]
        return sorted(versions, key=lambda x: x['last_modified'], reverse=True)

    def check_key_exists(self, path):
        """Check if an S3 object exists"""
        bucket, key = self.path_handler.parse_path(path)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            if hasattr(e, 'response'):
                error_code = e.response['Error']['Code']
                if error_code in ["404", "403"]:
                    return False
            raise 