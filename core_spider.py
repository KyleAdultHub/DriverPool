from .driver_pool import DriverPoll
from redis import StrictRedis
from .common.log import Logger
import threading
from queue import Queue
from collections import Iterable
import abc
from .common.killer import Killer
import time

_logger = Logger(__name__).logger


class Task(object):
    def __init__(self, handle_func, kw=None, url=None):
        self.kw = kw
        self.url = url
        self.handle_func = handle_func


class CoreSpider(abc.ABC):
    def __init__(self, concurrent=8, driver="chrome", proxy_url="", save_folder="./file_download",
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
        self.logger = _logger if not logger else logger

    def _task_pop(self):
        task = self._task_queue.get()
        if task:
            return task
        else:
            return None

    def _task_push(self, task):
        if task:
            self.logger.info("enqueue task kw:{} url:{}".format(task.kw, task.url))
        self._task_queue.put(task)

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
            self.logger.warning("fail to exchange driver, the deriver is not belong to driver pool")

    def _consumer(self):
        while True:
            task = self._task_pop()
            if task is None:
                self._task_queue.task_done()
                break
            handle_func = task.handle_func
            driver = self.driver_poll.query_driver()
            if isinstance(handle_func(task, driver), Iterable):
                for task_gen in handle_func(task, driver):
                    self._task_push(task_gen)
            self._task_queue.task_done()
            driver.delete_all_cookies()
            self.driver_poll.out_of_use(driver)

    def schedule(self):
        consumer_thd_list = [threading.Thread(target=self._consumer) for _ in range(self._concurrent)]
        self.init_task_enqueue()
        for thd in consumer_thd_list:
            thd.start()
        self.logger.info("CoreSpider schedule is started successfully, waiting for task to start.")
        while True:
            if self._task_queue.qsize() == 0:
                break
            if self.killed.kill_now:
                self.logger.info("receive kill signal, the producer is stopping.")
                for i in range(self._concurrent):
                    self._task_push(None)
                break
            time.sleep(1)
        self._task_queue.join()
        self.driver_poll.clear_driver_pool()


class CoreRedisSpider(abc.ABC):
    redis_key = ""

    def __init__(self, concurrent=4, driver="chrome", proxy_url="", save_folder="./file_download",
                 only_html=False, no_js=False, headless=False, driver_log_path="driver.log",
                 logger=None, proxy_scheme="http", timeout=60, driver_time_limit=60 * 5, driver_use_limit=8,
                 execute_path="", window_size=None, **redis_kwargs):
        self._concurrent = concurrent
        self._task_queue = Queue()
        self._redis_queue = self._create_redis_cursor(**redis_kwargs) if redis_kwargs else None
        if (not self.redis_key and self._redis_queue) or (self.redis_key and not self._redis_queue):
            raise Exception("the redis_key or redis args is None, redis_key:{redis_key}, kwargs:{kwargs}".format(
                redis_key=self.redis_key, kwargs=redis_kwargs
            ))
        self.driver_poll = DriverPoll(driver=driver, proxy_url=proxy_url, save_folder=save_folder, only_html=only_html,
                                      no_js=no_js, headless=headless, driver_log_path=driver_log_path, logger=logger,
                                      proxy_scheme=proxy_scheme, timeout=timeout, driver_size=concurrent,
                                      driver_time_limit=driver_time_limit, driver_use_limit=driver_use_limit,
                                      execute_path=execute_path, window_size=window_size)
        self.killed = Killer()
        self.logger = _logger if not logger else logger

    def _task_pop(self):
        task = self._task_queue.get()
        if task:
            return task
        else:
            return None

    def _task_push(self, task):
        if task:
            self.logger.info("enqueue task kw:{} url:{}".format(task.kw, task.url))
        self._task_queue.put(task)

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
                kw = self._redis_queue.blpop(self.redis_key)
                task = self.task_create(kw=kw)
                self._task_push(task)

    def _consumer(self):
        while True:
            task = self._task_pop()
            if task is None:
                self._task_queue.task_done()
                break
            handle_func = task.handle_func
            driver = self.driver_poll.query_driver()
            if isinstance(handle_func(task, driver), Iterable):
                for task_gen in handle_func(task, driver):
                    self._task_push(task_gen)
            self._task_queue.task_done()
            driver.delete_all_cookies()
            self.driver_poll.out_of_use(driver)

    def schedule(self):
        consumer_thd_list = [threading.Thread(target=self._consumer) for _ in range(self._concurrent)]
        init_task_enqueue_thd = threading.Thread(target=self.init_task_enqueue)
        init_task_enqueue_thd.setDaemon(True)
        init_task_enqueue_thd.start()
        for thd in consumer_thd_list:
            thd.start()
        self.logger.info("CoreRedisSpider schedule is started successfully, waiting for task to start.")
        while True:
            if self.killed.kill_now:
                self.logger.info("receive kill signal, the producer is stopping.")
                for i in range(self._concurrent):
                    self._task_push(None)
                break
            time.sleep(1)
        self._task_queue.join()
        self.driver_poll.clear_driver_pool()