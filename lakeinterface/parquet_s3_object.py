import polars as pl
from io import BytesIO

from lakeinterface.s3_object import S3Object


class ParquetS3Object(S3Object):

    def read_object(self):
        content = self.fetch_object()
        return pl.read_parquet(BytesIO(content.read()))
    
    def save_object(self, key, obj):
        with self.fs.open(f'{self.bucket}/{key}', mode='wb') as f:
            obj.write_parquet(f)
        