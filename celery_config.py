from celery import Celery
import os
import sys
from config import Config_Redis

# Add the project directory to the Python path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=Config_Redis.result_backend,
        broker=Config_Redis.broker_url
    )
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask
    return celery
