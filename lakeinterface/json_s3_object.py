import json
from lakeinterface.s3_object import S3Object
from lakeinterface.exceptions import LakeError


class JSONS3Object(S3Object):

    def read_object(self):
        content = self.fetch_object()
        try:
            return json.loads(content.read())
        except json.JSONDecodeError as e:
            raise LakeError(f"Failed to parse JSON content: {str(e)}")

    def save_object(self, key, obj, metadata=None):
        if not key.endswith('.json'):
            key = f'{key}.json'
        
        try:
            json_data = json.dumps(obj)
            resp = self.session.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json_data
            )
            status_code = resp['ResponseMetadata']['HTTPStatusCode']
            if status_code != 200:
                raise LakeError(f'Failed to save object. Status code: {status_code}')
            
            if metadata:
                self.update_metadata(metadata, overwrite=True)
                
        except Exception as e:
            raise LakeError(f'Failed to save object {key}: {str(e)}')
        
        
    