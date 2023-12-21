import polars as pl

from lakeinterface.s3_object import S3Object


class CSVS3Object(S3Object):

    def read_object(self):
        content = self.fetch_object()
        separator = self.kwargs.get('separator', ',')
        line_terminator = self.kwargs.get('line_terminator')
        if line_terminator:
            return pl.read_csv(content, separator=separator, lineterminator=line_terminator)
        else:
            return pl.read_csv(content, separator=separator)
        

    def save_object(self, key, obj):
        with self.fs.open(f'{self.bucket}/{key}', mode='wb') as f:
            obj.write_csv(f)
    