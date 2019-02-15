from driver_pool import DriverPoll


class Task(object):
    def __init__(self, kw=None, url=None):
        self.kw = kw
        self.url = url




class CoreSpider(object):
    def __init__(self, concurrent=8, redis_queue=None):
        self.concurrent = concurrent
        self.task_queue = list()
        self.redis_queue = redis_queue

    def add_task(self, kw=None, url=None):
        new_task = Task(kw, url)
        self.task_queue.append()

    def