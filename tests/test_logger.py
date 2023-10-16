import logging

from lakeinterface.logger import predefined_logger, log


def test_predefined_logger():
    logger = predefined_logger('test', ['console'])
    non_root_loggers = [logging.getLogger(name).name for name in logging.root.manager.loggerDict]

    assert 'test' in non_root_loggers


def test_log_output(caplog):
    logger = predefined_logger('test', ['console'])
    caplog.clear()
    logger.info('HELLO')
    assert caplog.record_tuples == [("test", logging.INFO, "HELLO")]


@log(logger_name='test3', exception_handling='pass')
def add(a,b, logger=None):
    logger.debug('FROM add function')
    logger.info('INFO add function')
    logger.error('ERROR add function')
    return a + b


decorator_logs = {
    ('test3', logging.DEBUG, 'function add called with args 1, 3, logger=<Logger test3 (DEBUG)>'),
    ('test3', logging.DEBUG, 'FROM add function'),
    ('test3', logging.INFO, 'INFO add function'),
    ('test3', logging.ERROR, 'ERROR add function')
}

def test_log_decorator(caplog):
    caplog.clear()
    add(1,3)

    assert set(caplog.record_tuples) == decorator_logs



