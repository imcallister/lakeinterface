from time import time
from io import BytesIO
import zipfile


def unzip(lake_interface, source_file, destination_folder, exclude_pattern=None, include_pattern=None):
    logs = [
        '-' * 30,
        f'Copying from {source_file} to {destination_folder}'
    ]
    
        
    zip_obj = lake_interface.get_object(source_file)
    buffer = BytesIO(zip_obj["Body"].read())

    z = zipfile.ZipFile(buffer)
    file_names = z.namelist()
    filtered_files = file_names.copy()

    if include_pattern:
        filtered_files = [f for f in filtered_files if include_pattern in f]

    if exclude_pattern:
        filtered_files = [f for f in filtered_files if exclude_pattern not in f]

    for filename in filtered_files:
        file_info = z.getinfo(filename)

        lake_interface.s3.upload_fileobj(
            Fileobj=z.open(filename),
            Bucket=lake_interface.bucket,
            Key=f'{destination_folder}/{filename}'
        )

        logs.append(f'Copied {filename}')

    return logs


def func_timer(func):
    # This function shows the execution time of 
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func
