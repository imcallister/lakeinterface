# Autogenerated by nbdev

d = { 'settings': { 'branch': 'main',
                'doc_baseurl': '/lakeinterface',
                'doc_host': 'https://imcallister.github.io',
                'git_url': 'https://github.com/imcallister/lakeinterface',
                'lib_path': 'lakeinterface'},
  'syms': { 'lakeinterface.athena': { 'lakeinterface.athena.Athena': ('athena.html#athena', 'lakeinterface/athena.py'),
                                      'lakeinterface.athena.Athena.__init__': ('athena.html#athena.__init__', 'lakeinterface/athena.py'),
                                      'lakeinterface.athena.Athena.get_data_catalog': ( 'athena.html#athena.get_data_catalog',
                                                                                        'lakeinterface/athena.py'),
                                      'lakeinterface.athena.Athena.get_query_results': ( 'athena.html#athena.get_query_results',
                                                                                         'lakeinterface/athena.py'),
                                      'lakeinterface.athena.Athena.get_query_status': ( 'athena.html#athena.get_query_status',
                                                                                        'lakeinterface/athena.py'),
                                      'lakeinterface.athena.Athena.start_query': ( 'athena.html#athena.start_query',
                                                                                   'lakeinterface/athena.py'),
                                      'lakeinterface.athena.LakeConfigNotFound': ( 'athena.html#lakeconfignotfound',
                                                                                   'lakeinterface/athena.py'),
                                      'lakeinterface.athena.parse_column_info': ( 'athena.html#parse_column_info',
                                                                                  'lakeinterface/athena.py'),
                                      'lakeinterface.athena.parse_table_info': ('athena.html#parse_table_info', 'lakeinterface/athena.py')},
            'lakeinterface.aurora': { 'lakeinterface.aurora.Aurora': ('aurora.html#aurora', 'lakeinterface/aurora.py'),
                                      'lakeinterface.aurora.Aurora.__exit__': ('aurora.html#aurora.__exit__', 'lakeinterface/aurora.py'),
                                      'lakeinterface.aurora.Aurora.__init__': ('aurora.html#aurora.__init__', 'lakeinterface/aurora.py'),
                                      'lakeinterface.aurora.Aurora.__new__': ('aurora.html#aurora.__new__', 'lakeinterface/aurora.py'),
                                      'lakeinterface.aurora.Aurora.aurora_connection': ( 'aurora.html#aurora.aurora_connection',
                                                                                         'lakeinterface/aurora.py'),
                                      'lakeinterface.aurora.Aurora.close_connections': ( 'aurora.html#aurora.close_connections',
                                                                                         'lakeinterface/aurora.py'),
                                      'lakeinterface.aurora.Aurora.open_read_connection': ( 'aurora.html#aurora.open_read_connection',
                                                                                            'lakeinterface/aurora.py'),
                                      'lakeinterface.aurora.Aurora.open_write_connection': ( 'aurora.html#aurora.open_write_connection',
                                                                                             'lakeinterface/aurora.py'),
                                      'lakeinterface.aurora.Aurora.query': ('aurora.html#aurora.query', 'lakeinterface/aurora.py')},
            'lakeinterface.config': { 'lakeinterface.config.ConfigManager': ('config.html#configmanager', 'lakeinterface/config.py'),
                                      'lakeinterface.config.ConfigManager.__init__': ( 'config.html#configmanager.__init__',
                                                                                       'lakeinterface/config.py'),
                                      'lakeinterface.config.ConfigManager._get_parameter': ( 'config.html#configmanager._get_parameter',
                                                                                             'lakeinterface/config.py'),
                                      'lakeinterface.config.ConfigManager.fetch_config': ( 'config.html#configmanager.fetch_config',
                                                                                           'lakeinterface/config.py')},
            'lakeinterface.core': {'lakeinterface.core.foo': ('core.html#foo', 'lakeinterface/core.py')},
            'lakeinterface.datalake': { 'lakeinterface.datalake.Datalake': ('s3.html#datalake', 'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.__init__': ( 's3.html#datalake.__init__',
                                                                                      'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.__new__': ( 's3.html#datalake.__new__',
                                                                                     'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.get': ('s3.html#datalake.get', 'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.get_object': ( 's3.html#datalake.get_object',
                                                                                        'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.list_objects': ( 's3.html#datalake.list_objects',
                                                                                          'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.load_csv': ( 's3.html#datalake.load_csv',
                                                                                      'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.load_json': ( 's3.html#datalake.load_json',
                                                                                       'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.load_parquet': ( 's3.html#datalake.load_parquet',
                                                                                          'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.most_recent': ( 's3.html#datalake.most_recent',
                                                                                         'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.put': ('s3.html#datalake.put', 'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.put_object': ( 's3.html#datalake.put_object',
                                                                                        'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.Datalake.save_json': ( 's3.html#datalake.save_json',
                                                                                       'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.S3ObjectNotFound': ( 's3.html#s3objectnotfound',
                                                                                     'lakeinterface/datalake.py'),
                                        'lakeinterface.datalake.most_recent': ('s3.html#most_recent', 'lakeinterface/datalake.py')},
            'lakeinterface.lake': { 'lakeinterface.lake.LakeConfigNotFound': ('datalake.html#lakeconfignotfound', 'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface': ('datalake.html#lakeinterface', 'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.__init__': ( 'datalake.html#lakeinterface.__init__',
                                                                                   'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.aurora_connection': ( 'datalake.html#lakeinterface.aurora_connection',
                                                                                            'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.get': ('datalake.html#lakeinterface.get', 'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.get_object': ( 'datalake.html#lakeinterface.get_object',
                                                                                     'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.get_query_results': ( 'datalake.html#lakeinterface.get_query_results',
                                                                                            'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.list_objects': ( 'datalake.html#lakeinterface.list_objects',
                                                                                       'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.load_csv': ( 'datalake.html#lakeinterface.load_csv',
                                                                                   'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.load_json': ( 'datalake.html#lakeinterface.load_json',
                                                                                    'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.most_recent': ( 'datalake.html#lakeinterface.most_recent',
                                                                                      'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.put': ('datalake.html#lakeinterface.put', 'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.put_object': ( 'datalake.html#lakeinterface.put_object',
                                                                                     'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.save_json': ( 'datalake.html#lakeinterface.save_json',
                                                                                    'lakeinterface/lake.py'),
                                    'lakeinterface.lake.LakeInterface.start_query': ( 'datalake.html#lakeinterface.start_query',
                                                                                      'lakeinterface/lake.py'),
                                    'lakeinterface.lake.aurora_query': ('datalake.html#aurora_query', 'lakeinterface/lake.py'),
                                    'lakeinterface.lake.get_data_catalog': ('datalake.html#get_data_catalog', 'lakeinterface/lake.py'),
                                    'lakeinterface.lake.parse_column_info': ('datalake.html#parse_column_info', 'lakeinterface/lake.py'),
                                    'lakeinterface.lake.parse_table_info': ('datalake.html#parse_table_info', 'lakeinterface/lake.py'),
                                    'lakeinterface.lake.unzip': ('datalake.html#unzip', 'lakeinterface/lake.py')},
            'lakeinterface.lake_utils': { 'lakeinterface.lake_utils.datalake_interface': ( 'datalake.html#datalake_interface',
                                                                                           'lakeinterface/lake_utils.py'),
                                          'lakeinterface.lake_utils.func_timer': ( 'datalake.html#func_timer',
                                                                                   'lakeinterface/lake_utils.py'),
                                          'lakeinterface.lake_utils.load_lake_interfaces': ( 'datalake.html#load_lake_interfaces',
                                                                                             'lakeinterface/lake_utils.py'),
                                          'lakeinterface.lake_utils.unzip': ('datalake.html#unzip', 'lakeinterface/lake_utils.py')},
            'lakeinterface.logger': { 'lakeinterface.logger.Logger': ('logger.html#logger', 'lakeinterface/logger.py'),
                                      'lakeinterface.logger.Logger.add_cloudwatch_handler': ( 'logger.html#logger.add_cloudwatch_handler',
                                                                                              'lakeinterface/logger.py'),
                                      'lakeinterface.logger.Logger.add_file_handler': ( 'logger.html#logger.add_file_handler',
                                                                                        'lakeinterface/logger.py'),
                                      'lakeinterface.logger.Logger.add_stream_handler': ( 'logger.html#logger.add_stream_handler',
                                                                                          'lakeinterface/logger.py'),
                                      'lakeinterface.logger.Logger.clear_all_handlers': ( 'logger.html#logger.clear_all_handlers',
                                                                                          'lakeinterface/logger.py'),
                                      'lakeinterface.logger.Logger.configure': ('logger.html#logger.configure', 'lakeinterface/logger.py'),
                                      'lakeinterface.logger.Logger.get_logger': ( 'logger.html#logger.get_logger',
                                                                                  'lakeinterface/logger.py'),
                                      'lakeinterface.logger._boto_filter': ('logger.html#_boto_filter', 'lakeinterface/logger.py'),
                                      'lakeinterface.logger.log': ('logger.html#log', 'lakeinterface/logger.py')},
            'lakeinterface.s3': { 'lakeinterface.s3.S3': ('s3.html#s3', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.__init__': ('s3.html#s3.__init__', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.__new__': ('s3.html#s3.__new__', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.get': ('s3.html#s3.get', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.get_object': ('s3.html#s3.get_object', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.list_objects': ('s3.html#s3.list_objects', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.load_csv': ('s3.html#s3.load_csv', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.load_json': ('s3.html#s3.load_json', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.most_recent': ('s3.html#s3.most_recent', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.put': ('s3.html#s3.put', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.put_object': ('s3.html#s3.put_object', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3.save_json': ('s3.html#s3.save_json', 'lakeinterface/s3.py'),
                                  'lakeinterface.s3.S3ObjectNotFound': ('s3.html#s3objectnotfound', 'lakeinterface/s3.py')},
            'lakeinterface.utilities': {'lakeinterface.utilities.func_timer': ('utilities.html#func_timer', 'lakeinterface/utilities.py')}}}
