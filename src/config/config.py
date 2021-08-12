import os


class ClickHouseConfig:
    host = ''
    port = ''
    user = ''
    password = ''
    db = ''
    secure = False
    verify = False
    compression = True


class Config:

    def __init__(self):
        self.use_db = True
        self.use_stdout = True
        self.use_separate_files = True
        self.ch_conn = 'http://localhost:8123'
        self.ch_table = 'desire_logs'
        self.buffer_capacity = 1
        self.build_in_keys_to_ch = ['message',
                               'levelname',
                               'filename',
                               'module',
                               'lineno,',
                               'exc_info',
                               'created',
                               'msecs',
                               'relativeCreated',
                               'asctime']
        self.build_in_log_keys = ['name',
                             'msg',
                             'args',
                             'levelname',
                             'levelno',
                             'pathname',
                             'filename',
                             'module',
                             'exc_info',
                             'exc_text',
                             'stack_info',
                             'lineno',
                             'funcName',
                             'created',
                             'msecs',
                             'relativeCreated',
                             'thread',
                             'threadName',
                             'processName',
                             'process']

        self.clickhouse = ClickHouseConfig()
        self.clickhouse.host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
        self.clickhouse.port = os.environ.get('CLICKHOUSE_PORT', 8123)
        self.clickhouse.db = os.environ.get('CLICKHOUSE_DB', 'clickhouse')
        self.clickhouse.user = os.environ.get('CLICKHOUSE_USER', 'root')
        self.clickhouse.password = os.environ.get('CLICKHOUSE_PASSWORD', 'password')