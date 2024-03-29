
import watchtower, logging, functools


def _boto_filter(record):
    # Filter log messages from botocore and its dependency, urllib3, in watchtower handler for CloudWatch.
    # This is required to avoid an infinite loop when shutting down.
    if record.name.startswith("botocore"):
        return False
    if record.name.startswith("urllib3"):
        return False
    return True

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


def clear_all_handlers(logger_name):
    logger = logging.getLogger(logger_name)
    while len(logger.handlers)>0:
        logger.removeHandler(logger.handlers[0])
        
        
def add_stream_handler(logger_name, handler_def):
    logger = logging.getLogger(logger_name)

    # check for existing stream handler
    if any(type(h)==logging.StreamHandler for h in logger.handlers):
        logger.debug('Console logging handler already attached')
        return

    c_handler = logging.StreamHandler()
    c_handler.setLevel(handler_def.get('level'))
    c_format = logging.Formatter(handler_def.get('format'))
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)
    logger.debug('Console logging handler added')
    return

def add_file_handler(logger_name, handler_def):
    logger = logging.getLogger(logger_name)

    # check for existing file handler
    if any(type(h)==logging.FileHandler for h in logger.handlers):
        logger.debug('File logging handler already attached')
        return

    f_handler = logging.FileHandler(handler_def.get('log_file_path'))
    f_handler.setLevel(handler_def.get('level'))
    f_format = logging.Formatter(handler_def.get('format'))
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
    logger.debug('File Logging handler added')
    return

def add_cloudwatch_handler(logger_name, handler_def):
    logger = logging.getLogger(logger_name)

    # check for existing cloudwatch handler
    if any(type(h)==watchtower.CloudWatchLogHandler for h in logger.handlers):
        logger.debug('Cloudwatch logging Handler already attached')
        return

    wtower_handler = watchtower.CloudWatchLogHandler(
        log_group_name=handler_def.get('log_group_name'),
        log_stream_name=handler_def.get('log_stream_name'),
        send_interval=10,
        create_log_group=False,
        boto3_profile_name=handler_def.get('aws_profile_name')
    )
    wtower_handler.setLevel(handler_def.get('level'))
    logger.addFilter(_boto_filter)

    w_format = logging.Formatter(handler_def.get('format'))
    wtower_handler.setFormatter(w_format)
    logger.addHandler(wtower_handler)
    logger.debug('Cloudwatch logging handler added')
    return


def configure_logger(logger_name, handlers_config):
    logger = logging.getLogger(logger_name)

    for h in handlers_config:
        match h['handler_type']:
            case 'stream':
                add_stream_handler(logger_name, h)
            case 'file':
                add_file_handler(logger_name, h)
            case 'cloudwatch':
                add_cloudwatch_handler(logger_name, h)

    logger.setLevel(min(h.level for h in logger.handlers))


def predefined_logger(logger_name, handlers):
    unsupported_handlers = [h for h in handlers if h not in LOG_HANDLERS]
    if len(unsupported_handlers) > 0:
        raise Exception(f"Unsupported handlers: {','.join(unsupported_handlers)}")
    
    if logger_name in [None, '']:
        raise Exception('Please specify logger name')
        
    configure_logger(logger_name, [LOG_HANDLERS.get(h) for h in handlers])
    return logging.getLogger(logger_name)


def log(logger_name='default', exception_handling=None):
    
    def decorator_log(func):
        
        handler_config = [
            {
                'handler_type': 'stream', 
                'level': logging.DEBUG, 
                'format': '%(name)s - %(levelname)s - %(message)s'
            }
        ]

        configure_logger(logger_name, handler_config)
        logger = logging.getLogger(logger_name)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            kwargs['logger'] = logger
            
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)
            logger.debug(f"function {func.__name__} called with args {signature}")
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
                if exception_handling == 'pass':
                    return
                else:
                    raise e
        return wrapper

    return decorator_log

