import time
import inspect
import sys
import logging
import requests
import json
from logging.handlers import BufferingHandler
import traceback

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



class CallStackFormatter(logging.Formatter):

    def formatStack(self, _ = None) -> str:
        stack = inspect.stack()[::-1]
        stack_names = (inspect.getmodulename(stack[0].filename),
                       *(frame.function
                         for frame
                         in stack[1:-9]))
        return '::'.join(stack_names)

    def format(self, record):
        record.message = record.getMessage()
        record.stack_info = self.formatStack()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        s = self.formatMessage(record)
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + record.exc_text
        return s


class DesireLoggerHandler(BufferingHandler):

    source_uuid = None
    messages_ts = []

    def __init__(self,
                 ch_conn='http://localhost:8123',
                 ch_table=None,
                 logging_build_in_columns_to_ch=None):
        #
        self._config = Config()
        #
        super().__init__(self._config.buffer_capacity)
        #
        if ch_conn is None:
            ch_conn = f'http://{self._config.clickhouse.host}:{self._config.clickhouse.port}/'
        self._ch_conn = ch_conn
        #
        if ch_table is None:
            ch_table = self._config.ch_table
        self._ch_table = ch_table
        #
        BufferingHandler.__init__(self, self._config.buffer_capacity)

        #
        if logging_build_in_columns_to_ch is None:
            self._build_in_keys_to_ch = self._config.build_in_keys_to_ch
        else:
            self._build_in_keys_to_ch = logging_build_in_columns_to_ch
        #
        self.build_in_log_keys = self._config.build_in_log_keys
        self._auth = (self._config.clickhouse.user, self._config.clickhouse.password)
        #
        try:
            sql = f"CREATE TABLE IF NOT EXISTS {self._ch_table} (" \
                  f"name String, " \
                  f"source_uuid String, " \
                  f"msg String, " \
                  f"levelname String, " \
                  f"pathname String, " \
                  f"filename String, " \
                  f"module String, " \
                  f"lineno UInt16, " \
                  f"funcName String, " \
                  f"dt DateTime ('UTC') DEFAULT now()) " \
                  f"engine MergeTree() " \
                  f"PARTITION BY toYYYYMM(dt) " \
                  f"ORDER BY name;"
            res = requests.post(url=self._ch_conn,
                                params={'query': sql,
                                        'database': self._config.clickhouse.db,
                                        'input_format_skip_unknown_fields': 1},
                                auth=self._auth)
            res.raise_for_status()
        except requests.exceptions.RequestException as ex:
            print(f'{ex}')

    def flush(self):
        self.acquire()

        try:
            if len(self.buffer) > 0:
                _data = ''
                for record in self.buffer:
                    message_dict = record.__dict__.copy()
                    if record.exc_info:
                        ex = record.exc_info
                        message_dict['exc_info'] = ('\n'.join(traceback.format_exception(etype=ex[0],
                                                                                         value=ex[1],
                                                                                         tb=ex[2])))
                        try:
                            if record.stack_info and not message_dict.get('stack_info'):
                                stack_formatter = CallStackFormatter()
                                message_dict['stack_info'] = stack_formatter.formatStack(record.stack_info)
                        except Exception as ex:
                            print(f'{ex}')

                        for key in list(message_dict.keys()):
                            if key in self.build_in_log_keys and key not in self._build_in_keys_to_ch:
                                del message_dict[key]

                    _data += json.dumps(message_dict) + '\n'

                    if message_dict:
                        data_ch = dict()
                        data_ch["name"] = message_dict.get("name")
                        data_ch["msg"] = message_dict.get("msg")
                        data_ch["levelname"] = message_dict.get("levelname")
                        data_ch["pathname"] = message_dict.get("pathname")
                        data_ch["filename"] = message_dict.get("filename")
                        data_ch["module"] = message_dict.get("module")
                        data_ch["lineno"] = message_dict.get("lineno")
                        data_ch["funcName"] = message_dict.get("funcName")

                        ts = str(message_dict.get("created"))

                        found = False

                        for mt in self.messages_ts:
                            if mt in ts:
                                found = True

                        if not found:
                            self.messages_ts.append(ts)
                            sql = f'insert into {self._ch_table} format JSONEachRow'

                            try:
                                res = requests.post(url=self._ch_conn,
                                                    params={'query': sql,
                                                            'database': self._config.clickhouse.db,
                                                            'input_format_skip_unknown_fields': 1},
                                                    data=json.dumps(data_ch),
                                                    auth=self._auth)
                                res.raise_for_status()
                            except requests.exceptions.RequestException as ex:
                                print(f'{ex}')
                                record.__dict__['msg'] = res.__dict__.get('_content', 'There is no response text.')
                                self.handleError(record)
        except Exception as ex:
            print(f'{ex}')
        finally:
            self.release()


def getLogger(name,
              filename=None,
              file_handler_format='%(levelname) -10s %(asctime)s %(module)s:%(lineno)s %(funcName)s %(message)s',
              file_handler_date_format='%Y-%m-%d %H:%M:%S',
              stdout_handler_format='[%(asctime)s] - %(levelname)-8s: %(name)-60s: %(funcName)-30s: %(message)s',
              stdout_handler_date_format='%Y-%m-%d %H:%M:%S',
              level=logging.INFO,
              ch_conn=None,
              ch_table=None):

    logger = logging.getLogger(name)

    if Config().use_db:
        ch_handler = DesireLoggerHandler(ch_conn=ch_conn,
                                         ch_table=ch_table)
        logger.addHandler(ch_handler)

    if filename:
        file_handler = logging.FileHandler(filename)
        formatter = logging.Formatter(file_handler_format, file_handler_date_format)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if Config().use_stdout:
        stdout_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(stdout_handler_format,
                                      stdout_handler_date_format)
        stdout_handler.setFormatter(formatter)
        logger.addHandler(stdout_handler)

    logger.setLevel(level)

    return logger


def main():
    logger = getLogger('desire',
                       filename='log.log')

    logger.info('info test')
    logger.error('error test')
    logger.warning('warning test')

    return 0


if __name__ == '__main__':
    sys.exit(main())