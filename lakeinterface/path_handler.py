from datetime import datetime
from dateutil.parser import parse
from lakeinterface.exceptions import LakeError

class S3PathHandler:
    @staticmethod
    def parse_path(path, file_type=None):
        """Parse S3 path into bucket and key components"""
        path = path.replace('s3://', '')
        if path.endswith('/'):
            path = path[:-1]

        parts = path.split('/')
        if file_type and not parts[-1].endswith(f'.{file_type}'):
            parts[-1] += f'.{file_type}'

        return (parts[0], parts[1]) if len(parts) == 2 else (parts[0], '/'.join(parts[1:]))

    @staticmethod
    def is_timestamp(ts):
        """Check if string is a valid timestamp"""
        try:
            return isinstance(parse(ts), datetime)
        except:
            return False

    @staticmethod
    def find_most_recent(keys, path):
        """Find most recent file from a list of keys with timestamps"""
        file_types = {o.split('/')[-1] for o in keys}
        
        if len(file_types) > 1:
            raise LakeError(f"Mixed filetypes found with path {path}: {','.join(file_types)}")
        
        file_type = next(iter(file_types))
        dates = [
            o.replace(path, '').replace(file_type, '').replace('/', '')
            for o in keys
        ]

        if len({len(d) for d in dates}) > 1:
            raise LakeError(f'Mixed timestamp formats found with path {path}')
            
        latest = max(dates)
        return f'{path}/{latest}/{file_type}' 