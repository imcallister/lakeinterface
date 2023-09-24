# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/10_datalake.ipynb.

# %% auto 0
__all__ = ['CONSOLE_LOG_HANDLER', 'CLOUDWATCH_LOG_HANDLER', 'LOG_HANDLERS', 'SUPPORTED_INTERFACES', 'SUPPORTED_LOG_HANDLERS',
           'load_lake_interfaces', 'datalake_interface', 'unzip', 'func_timer']

# %% ../nbs/10_datalake.ipynb 3
import logging
from time import time
import functools

from lakeinterface.logger import Logger
from lakeinterface.config import ConfigManager
from lakeinterface.datalake import Datalake
from lakeinterface.aurora import Aurora

# %% ../nbs/10_datalake.ipynb 4
CONSOLE_LOG_HANDLER = {
    'handler_type': 'stream', 
    'level': logging.INFO, 
    'format': '%(name)s - %(levelname)s - %(message)s'
}

CLOUDWATCH_LOG_HANDLER = {
    'handler_type': 'cloudwatch', 
    'log_group_name': 'machinesp/test', 
    'log_stream_name': 'lake_tester', 
    'level': logging.DEBUG, 
    'format': '%(levelname)s - %(message)s'
}

LOG_HANDLERS = {
    'console': CONSOLE_LOG_HANDLER,
    'cloudwatch': CLOUDWATCH_LOG_HANDLER
}


SUPPORTED_INTERFACES = {'lake','aurora'}
SUPPORTED_LOG_HANDLERS = set(LOG_HANDLERS.keys())

# %% ../nbs/10_datalake.ipynb 5
def load_lake_interfaces(
    config_name='bankdata',
    logger_name='bankdata',
    aws_profile=None,
    interface_names=[],
    log_handlers=[]
):
            
    unsupported_interfaces = list(set(interface_names) - SUPPORTED_INTERFACES)
    if len(unsupported_interfaces) > 0:
        raise Exception(f'Unsupported interfaces.\
            You passed {",".join(unsupported_interfaces)}.\
            Following are supported:{",".join(SUPPORTED_INTERFACES)}')
        
    unsupported_log_handlers = list(set(log_handlers) - SUPPORTED_LOG_HANDLERS)
    if len(unsupported_log_handlers) > 0:
        raise Exception(f'Unsupported log handlers.\
            You passed {",".join(unsupported_log_handlers)}.\
            Following are supported:{",".join(SUPPORTED_LOG_HANDLERS)}')
    
    if len(log_handlers) > 0:
        logger = Logger()
        logger.configure(
            [LOG_HANDLERS.get(h) for h in log_handlers], 
            logger_name=logger_name
        )

    cfgmgr = ConfigManager(profile=aws_profile)
    cfg = cfgmgr.fetch_config(config_name)
    
    interfaces = {}
    if 'lake' in interface_names:
        interfaces['lake'] = Datalake(cfg, profile_name=aws_profile)
    
    if 'aurora' in interface_names:
        interfaces['aurora'] = Aurora(cfg)
        
    return interfaces

# %% ../nbs/10_datalake.ipynb 9
def datalake_interface(config_name='bankdata', log_handlers=['console']):
    
    def inner(func):
        li = load_lake_interfaces(
            config_name=config_name,
            logger_name='bankdata',
            interface_names=['lake'],
            log_handlers=log_handlers
        )

        datalake = li['lake']

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            kwargs['datalake'] = datalake
            return func(*args, **kwargs)

        return wrapper

    return inner
    

# %% ../nbs/10_datalake.ipynb 14
from io import BytesIO
import zipfile

# %% ../nbs/10_datalake.ipynb 15
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


# %% ../nbs/10_datalake.ipynb 17
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
