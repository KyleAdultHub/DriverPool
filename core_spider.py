from driver_pool import DriverPoll
from redis import StrictRedis
from common.log import Logger
import threading
from queue import Queue
import abc
from common.killer import Killer

try:
    import cPickle as pickle
except ImportError:
    import pickle
import time

_logger = Logger(__name__).logger


class Task(object):
    def __init__(self, handle_func, kw=None, url=None):
        self.kw = kw
        self.url = url
        self.handle_func = handle_func


class CoreSpider(abc.ABC):
    def __init__(self, concurrent=8, driver="chrome", proxy_url="", save_folder="./",
                 only_html=False, no_js=False, headless=False, driver_log_path="driver.log",
                 logger=None, proxy_scheme="http", timeout=60, driver_time_limit=60 * 5, driver_use_limit=8,
                 execute_path="", window_size=None):
        self._concurrent = concurrent
        self._task_queue = Queue()
        self.driver_poll = DriverPoll(driver=driver, proxy_url=proxy_url, save_folder=save_folder, only_html=only_html,
                                      no_js=no_js, headless=headless, driver_log_path=driver_log_path, logger=logger,
                                      proxy_scheme=proxy_scheme, timeout=timeout, driver_size=concurrent,
                                      driver_time_limit=driver_time_limit, driver_use_limit=driver_use_limit,
                                      execute_path=execute_path, window_size=window_size)
        self.killed = Killer()

    def _task_pop(self):
        task_pickle = self._task_queue.get()
        if task_pickle:
            return pickle.loads(task_pickle)
        else:
            return None

    def _task_push(self, task):
        if task:
            _logger.info("enqueue task kw:{} url:{}".format(task.kw, task.url))
        self._task_queue.put(pickle.dumps(task, protocol=-1))

    @abc.abstractmethod
    def task_create(self):
        """该方法为初始化任务方法，继承的爬虫必须实现该方法，
        当使用redis作为生产者的时候，调度器每次收到redis一个任务就会调用一次该方法，
        并把收到的内容作用kw参数， 传递给该方法，
        返回值: 一个Task对象
        """

    def init_task_enqueue(self):
        task = self.task_create()
        self._task_push(task)

    def exchange_driver(self, driver):
        if self.driver_poll.dequeue_driver(driver):
            return self.driver_poll.query_driver()
        else:
            _logger.warning("fail to exchange driver, the deriver is not belong to driver pool")

    def _consumer(self):
        while True:
            task = self._task_pop()
            if task is None:
                break
            handle_func = task.handle_func
            driver = self.driver_poll.query_driver()
            for task_gen in handle_func(task, driver):
                self._task_push(task_gen)
            driver.delete_all_cookies()
            self.driver_poll.out_of_use(driver)

    def schedule(self):
        consumer_thd_list = [threading.Thread(target=self._consumer) for _ in range(self._concurrent)]
        init_task_enqueue_thd = threading.Thread(target=self.init_task_enqueue)
        init_task_enqueue_thd.setDaemon(True)
        init_task_enqueue_thd.start()
        for thd in consumer_thd_list:
            thd.start()
        while True:
            if self.killed.kill_now:
                _logger.info("receive kill signal, the producer is stopping.")
                for i in range(self._concurrent):
                    self._task_push(None)
                break
            time.sleep(1)
        self._task_queue.join()
        self.driver_poll.clear_driver_pool()


class CoreRedisSpider(abc.ABC, CoreSpider):
    def __init__(self, concurrent=8, driver="chrome", proxy_url="", save_folder="./",
                 only_html=False, no_js=False, headless=False, driver_log_path="driver.log",
                 logger=None, proxy_scheme="http", timeout=60, driver_time_limit=60 * 5, driver_use_limit=8,
                 execute_path="", window_size=None, redis_key=None, **redis_kwargs):
        self._concurrent = concurrent
        self._task_queue = Queue()
        self._redis_queue = self._create_redis_cursor(**redis_kwargs) if redis_kwargs else None
        self._redis_key = redis_key
        if (not self._redis_key and self._redis_queue) or (self._redis_key and not self._redis_queue):
            raise Exception("the redis_key or redis args is None, redis_key:{redis_key}, kwargs:{kwargs}".format(
                redis_key=self._redis_key, kwargs=redis_kwargs
            ))
        self.driver_poll = DriverPoll(driver=driver, proxy_url=proxy_url, save_folder=save_folder, only_html=only_html,
                                      no_js=no_js, headless=headless, driver_log_path=driver_log_path, logger=logger,
                                      proxy_scheme=proxy_scheme, timeout=timeout, driver_size=concurrent,
                                      driver_time_limit=driver_time_limit, driver_use_limit=driver_use_limit,
                                      execute_path=execute_path, window_size=window_size)
        self.killed = Killer()

    @staticmethod
    def _create_redis_cursor(**redis_kwargs):
        return StrictRedis(**redis_kwargs)

    @abc.abstractmethod
    def task_create(self, kw):
        """该方法为初始化任务方法，继承的爬虫必须实现该方法，
        当使用redis作为生产者的时候，调度器每次收到redis一个任务就会调用一次该方法，
        并把收到的内容作用kw参数， 传递给该方法，
        返回值: 一个Task对象
        """

    def init_task_enqueue(self):
        while True:
            if self._task_queue.qsize() < self._concurrent:
                kw = self._redis_queue.lpop(self._redis_key)
                task = self.task_create(kw=kw)
                self._task_push(task)