import json

from lakeinterface.s3_object import S3Object


class JSONS3Object(S3Object):

    def read_object(self):
        content = self.fetch_object()
        return json.loads(content.read())

    def save_object(self, key, obj):
        try:
            resp = self.session.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(obj)
            )
            status_code = resp['ResponseMetadata']['HTTPStatusCode']
            if status_code == 200:
                return True
            else:
                raise Exception(f'Unknown error. Status code: {status_code}')
        except Exception as e:
            raise Exception(f'Unknown error in put object for {key}. {str(e)}')
    