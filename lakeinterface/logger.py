# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/01a_logger.ipynb.

# %% auto 0
__all__ = ['Logger', 'log']

# %% ../nbs/01a_logger.ipynb 2
import watchtower, logging, functools

# %% ../nbs/01a_logger.ipynb 3
def _boto_filter(record):
    # Filter log messages from botocore and its dependency, urllib3, in watchtower handler for CloudWatch.
    # This is required to avoid an infinite loop when shutting down.
    if record.name.startswith("botocore"):
        return False
    if record.name.startswith("urllib3"):
        return False
    return True

# %% ../nbs/01a_logger.ipynb 4
class Logger:
    """
    A class to wrap standard python logger but adding a handler to write to AWS CloudWatch
    ...

    Methods
    -------
    __init__(base_level)
        Sets base config
    
    configure_logger(logger_name, handlers_config, profile_name='default'):
        Initializes the logger and add any handlers defined in the handlers_confif
    
    add_stream_handler(handler_def):
        Add a handler for logging to console
    
    add_file_handler(handler_def):
        Add a handler for logging to file
    
    add_cloudwatch_handler(handler):
        Add a handler for logging to AWS Cloud Watch

    debug(msg) / info(msg) / warning(msg) / error(msg):
        Write logs of the relevant level
        
    clear_all_handlers():
        Remove all handlers from logger
    """
    
#     def __init__(self, base_level=logging.ERROR):
#         logging.basicConfig(level=base_level)
        
    def get_logger(self, name=None):
        if name:
            return logging.getLogger(name)
        else:
            return logging.getLogger()
    
    def configure(self, handlers_config, logger_name=None):
        logger = self.get_logger(logger_name)
        
        for h in handlers_config:
            match h['handler_type']:
                case 'stream':
                    self.add_stream_handler(h, logger_name=logger_name)
                case 'file':
                    self.add_file_handler(h, logger_name=logger_name)
                case 'cloudwatch':
                    self.add_cloudwatch_handler(h, logger_name=logger_name)
                    
        logger.setLevel(min(h.level for h in logger.handlers))

    def add_stream_handler(self, handler_def, logger_name=None):
        logger = self.get_logger(logger_name)
        
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
        
    def add_file_handler(self, handler_def, logger_name=None):
        logger = self.get_logger(logger_name)
        
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
        
    def add_cloudwatch_handler(self, handler_def, logger_name=None):
        logger = self.get_logger(logger_name)
        
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

    def clear_all_handlers(self, logger_name=None):
        logger = self.get_logger(logger_name)
        while len(logger.handlers)>0:
            logger.removeHandler(logger.handlers[0])

# %% ../nbs/01a_logger.ipynb 5
def log(logger_name=None, exception_handling=None):
    
    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = Logger().get_logger(logger_name)

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

