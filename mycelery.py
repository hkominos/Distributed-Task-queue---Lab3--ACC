from __future__ import absolute_import

from celery import Celery

app = Celery('tasks',
             broker='amqp://hako:hako@192.168.0.86::5672',
             backend='amqp://',
             include=['tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
)

if __name__ == '__main__':
    app.start()

