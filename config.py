import os
from datetime import timedelta
from json import JSONEncoder as BaseJSONEncoder
import datetime

class JSONEncoder(BaseJSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, datetime.datetime):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return super().default(obj)

class Config:
    DEBUG = True
    SECRET_KEY = os.urandom(24)
    JWT_SECRET_KEY = os.urandom(24)
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=1)
    JSON_ENCODER = JSONEncoder

class Config_Redis:
    DEBUG = True
    SECRET_KEY = os.urandom(24)
    JWT_SECRET_KEY = os.urandom(24)
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=1)
    JSON_ENCODER = JSONEncoder
    result_backend = 'redis://localhost:6379/0'
    broker_url = 'redis://localhost:6379/0'
