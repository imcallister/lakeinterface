from io import BytesIO
import zipfile
from lakeinterface.exceptions import UnsupportedFileType, LakeError
from lakeinterface.path_handler import S3PathHandler

class S3ObjectOperations:
    def __init__(self, s3_client, file_system):
        self.client = s3_client
        self.fs = file_system
        self.path_handler = S3PathHandler()

    def upload_file(self, file_obj, destination_path, metadata=None):
        """Upload a file object to S3"""
        bucket, key = self.path_handler.parse_path(destination_path)
        extra_args = {"Metadata": metadata} if metadata else {}
        
        return self.client.upload_fileobj(
            Fileobj=file_obj,
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args
        )

    def unzip_to_s3(self, zip_obj, destination_folder):
        """Extract zip contents directly to S3"""
        buffer = BytesIO(zip_obj.read())
        with zipfile.ZipFile(buffer) as z:
            for filename in z.namelist():
                self.upload_file(
                    z.open(filename),
                    f'{destination_folder}/{filename}'
                )

    def copy_object(self, from_path, to_path, metadata=None):
        """Copy an S3 object to a new location"""
        from_bucket, from_key = self.path_handler.parse_path(from_path)
        to_bucket, to_key = self.path_handler.parse_path(to_path)
        
        copy_source = {
            'Bucket': from_bucket,
            'Key': from_key
        }
        
        copy_args = {
            'Bucket': to_bucket,
            'Key': to_key,
            'CopySource': copy_source,
        }

        if metadata is not None:
            copy_args.update({
                'Metadata': metadata,
                'MetadataDirective': 'REPLACE'
            })
        else:
            copy_args['MetadataDirective'] = 'COPY'

        return self.client.copy_object(**copy_args) 