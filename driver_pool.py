# -*- coding: utf-8 -*-
import json
import os
import re
import threading
import time
from random import randint
import requests
from selenium import webdriver
from .common.log import Logger

from .common.killer import killer

_logger = Logger(__name__).logger


class DriverPoll(object):
    _lock = threading.RLock()

    def __init__(self, driver="chrome", proxy_url="", save_folder="./", only_html=False,
                 no_js=False, headless=False, driver_log_path="driver.log", logger=None,
                 proxy_scheme="http", timeout=60, driver_size=4, driver_time_limit=60*5,
                 driver_use_limit=8, execute_path="", window_size=None):
        """
        :param driver:  only can be set to chrome or firefox
        :param proxy_url: the url can get proxy list, choose the first one proxy use for driver
        :param save_folder: the folder save files that download
        :param only_html: only parse html
        :param no_js: do not parse javascript code
        :param headless: headless mode , default False
        :param driver_log_path: the log of drover
        :param logger: the log logger, default
        :param proxy_scheme: the proxy scheme, default http
        :param timeout: driver request timeout setting, default 60 seconds
        :param driver_size: the driver pool size , default 4
        :param driver_time_limit: per driver spend time limit
        :param driver_use_limit: per driver use time limit
        """
        self._kill = killer
        self.driver_pool = []
        self._driver_use_times_list = []
        self._driver_query_time_list = []
        self._driver_using_flag = []
        self._driver_save_path_list = []
        self.proxy_url = proxy_url
        self.save_folder = save_folder
        self.driver_engine = driver
        self.only_html = only_html
        self.no_js = no_js
        self.headless = headless
        self.driver_log_path = driver_log_path
        self.logger = _logger if not logger else logger
        self.proxy_scheme = proxy_scheme
        self.timeout = timeout
        self.driver_size = driver_size
        self.driver_time_limit = driver_time_limit
        self.driver_use_limit = driver_use_limit
        self._save_path_list = [os.path.join(save_folder, str(dir_name)) for dir_name in range(1, self.driver_size+1)]
        self.execute_path = execute_path
        self.window_size = window_size if window_size else None

    def _get_save_path(self):
        self._lock.acquire()
        if not self._save_path_list:
            self._save_path_list = [os.path.join(self.save_folder, str(dir_name)) for dir_name in
                                   range(1, self.driver_size + 1)]
        save_path = self._save_path_list.pop(0)
        self._lock.release()
        return save_path

    def _get_proxy(self):
        response = requests.get(self.proxy_url)
        if re.search(r'20000', response.content.decode(), re.S):
            ip_list = json.loads(response.content.decode(u'utf-8'))[u'data'][u'ips']
            if not ip_list:
                raise Exception("get none ip")
            return ip_list[0][u'ip'], str(ip_list[0][u'port'])

    def get_proxy(self):
        while True:
            if self._kill.kill_now:
                break
            try:
                _proxy = self._get_proxy()
                return _proxy[0], _proxy[1]
            except Exception as e:
                self.logger.warning('Failed to get proxy_ip from proxy_url:{proxy_url}, error:{err}'.format(
                    proxy_url=self.proxy_url, err=e))
                time.sleep(1.11111)
                continue

    def _get_one_driver(self, save_path="./"):
        driver = None
        kwargs = {}
        if self.execute_path:
            kwargs.update({"executable_path": self.execute_path})
        if self.driver_engine == "firefox":
            firefox_options = webdriver.FirefoxOptions()
            if self.only_html:
                firefox_options.set_preference(u'permissions.default.stylesheet', 2)
                firefox_options.set_preference(u'permissions.default.image', 2)
            if self.no_js:
                firefox_options.set_preference(u'dom.ipc.plugins.enabled.libflashplayer.so', u'false')
            if self.headless:
                firefox_options.add_argument("--headless")
            if self.save_folder:
                if not os.path.exists(save_path):
                    os.makedirs(save_path)
                self.clear_download_path(save_path)
                firefox_options.set_preference("browser.download.folderList", 2)
                firefox_options.set_preference("browser.download.dir", save_path)
                firefox_options.set_preference('browser.download.manager.showWhenStarting', False)
                firefox_options.set_preference('browser.helperApps.neverAsk.saveToDisk',
                                               'application/x-csv, application/x-txt,application/zip,text/plain,\
                                               application/vnd.ms-excel,text/csv,text/comma-separated-values,\
                                               application/octet-stream,application/vnd.openxmlformats-officedocument.\
                                               spreadsheetml.sheet,application/vnd.openxmlformats-officedocument.\
                                               wordprocessingml.document')
            if self.proxy_url:
                _ip, _port = self.get_proxy()
                firefox_profile = webdriver.FirefoxProfile()
                firefox_profile.set_preference("network.proxy.type", 1)
                firefox_profile.set_preference("network.proxy.http", _ip)
                firefox_profile.set_preference("network.proxy.http_port", int(_port))
                firefox_profile.update_preferences()
            else:
                firefox_profile = None
            driver = webdriver.Firefox(firefox_options=firefox_options, firefox_profile=firefox_profile,
                                       service_log_path=self.driver_log_path, **kwargs)
            driver.set_page_load_timeout(self.timeout)
            driver.save_path = save_path
            driver.pid = time.time()
        elif self.driver_engine == "chrome":
            chrome_options = webdriver.ChromeOptions()
            if self.only_html:
                prefs = {
                    'profile.default_content_setting_value.images': 2,
                    'profile.content_settings.plugin_whitelist.adobe-flash-player': 2,
                    'profile.content_settings.exceptions.plugins.*,*.per_resource.adobe-flash-player': 2
                }
                chrome_options.add_experimental_option('prefs', prefs)
            if self.no_js:
                prefs = {
                    'profile.default_content_setting_value.javascript': 2,
                }
                chrome_options.add_experimental_option('prefs', prefs)
            if self.headless:
                chrome_options.add_argument("--headless")
            if self.save_folder:
                if not os.path.exists(save_path):
                    os.makedirs(save_path)
                self.clear_download_path(save_path)
                prefs = {
                    'profile.default_content_settings.popups': 0,
                    "download.default_directory": save_path
                }
                chrome_options.add_experimental_option("prefs", prefs)
            if self.proxy_url:
                _ip, _port = self.get_proxy()
                proxy = "{}://{}:{}".format(self.proxy_scheme, _ip, _port)
                chrome_options.add_argument("--proxy-server={chrome_proxy}".format(chrome_proxy=proxy))
            driver = webdriver.Chrome(chrome_options=chrome_options,
                                      service_log_path=self.driver_log_path,  **kwargs)
            driver.set_page_load_timeout(self.timeout)
            driver.save_path = save_path
            driver.pid = time.time()
        if self.window_size and driver:
            driver.set_window_size(self.window_size[0], self.window_size[1])
        return driver

    def gen_one_driver(self, save_path="./"):
        while True:
            if self._kill.kill_now:
                break
            try:
                driver = self._get_one_driver(save_path)
                return driver
            except Exception as e:
                self.logger.warning('Fail to get {driver_name} driver, the error info is {err}, try again.'.format(
                    driver_name=self.driver_engine, err=e
                ))
                time.sleep(1.11111)
                continue

    def goto(self, driver, url, retry_times=0):
        err_times = 0
        while True:
            if self._kill.kill_now:
                break
            try:
                driver.get(url)
                return driver.page_source
            except Exception as e:
                time.sleep(1.11111)
                err_times += 1
                if err_times > retry_times:
                    raise
                continue

    def _enqueue_driver(self, driver):
        self._lock.acquire()
        if len(self.driver_pool) >= self.driver_size:
            self.logger.warning('driver pool is full, can not enqueue driver now.')
        else:
            self.driver_pool.append(driver)
            self._driver_query_time_list.append(time.time())
            self._driver_use_times_list.append(0)
            self._driver_using_flag.append(False)
            self.logger.debug('Successfully enqueue new driver into the driver pool, driver pool size now: {size}'.format(
                size=len(self.driver_pool)
            ))
        self._lock.release()

    def quit_driver(self, driver, dont_output=False):
        for times in range(3):
            try:
                driver.quit()
                break
            except Exception as e:
                if not dont_output:
                    self.logger.warning('Fail to close this driver, error info: {}'.format(e))

    def dequeue_driver(self, driver):
        self._lock.acquire()
        if driver not in self.driver_pool:
            self.logger.warning('the driver you want to dequeue is not in the driver pool, can not be dequeue.')
            return False
        else:
            driver_index = self.driver_pool.index(driver)
            self.driver_pool.pop(driver_index)
            self._driver_use_times_list.pop(driver_index)
            self._driver_query_time_list.pop(driver_index)
            self._driver_using_flag.pop(driver_index)
            self.quit_driver(driver)
            self.logger.debug('Successfully dequeue this driver from driver pool, driver pool size now: {size}'.format(
                size=len(self.driver_pool)
            ))
        self._lock.release()
        return True

    def _get_driver_from_driver_pool(self):
        while True:
            if self._kill.kill_now:
                break
            _rand_int = randint(0, self.driver_size)
            _using_flag = self._driver_using_flag[_rand_int]
            if _using_flag:
                time.sleep(1.11111)
                continue
            else:
                _driver_use_times = self._driver_use_times_list[_rand_int]
                _driver_spend_time = time.time() - self._driver_query_time_list[_rand_int]
                driver = self.driver_pool[_rand_int]
                if _driver_use_times > self.driver_use_limit or _driver_spend_time > self.driver_time_limit:
                    self.dequeue_driver(driver)
                    save_path = self._get_save_path()
                    driver = self.gen_one_driver(save_path)
                    self._enqueue_driver(driver)
                    self.clear_download_path(save_path)
                    return driver, -1
                else:
                    return driver, _rand_int

    @staticmethod
    def clear_download_path(save_path):
        if os.listdir(save_path):
            for file_path in [os.path.join(save_path, file_name) for file_name in os.listdir(save_path)]:
                os.remove(file_path)

    def query_driver(self):
        self._lock.acquire()
        if len(self.driver_pool) < self.driver_size:
            save_path = self._get_save_path()
            driver = self.gen_one_driver(save_path=save_path)
            self._enqueue_driver(driver)
            self._driver_use_times_list[-1] += 1
            self._driver_using_flag[-1] = True
        else:
            driver, index = self._get_driver_from_driver_pool()
            self._driver_use_times_list[index] += 1
            self._driver_using_flag[index] = True
        self._lock.release()
        return driver

    def out_of_use(self, driver):
        self._lock.acquire()
        if driver not in self.driver_pool:
            self.logger.warning('the driver you run out is not in the driver pool, can not be set to not using.')
        else:
            driver_index = self.driver_pool.index(driver)
            self._driver_using_flag[driver_index] = False
            self.logger.debug('Successfully transfer thr flag for the driver to no using, driver pool size now: {size}'.format(
                size=len(self.driver_pool)
            ))
        self._lock.release()

    @staticmethod
    def get_download_filepath_list(driver):
        """
        :rtype: list
        """
        save_path = driver.save_path
        file_path_list = [os.path.join(save_path, file_name) for file_name in os.listdir(save_path)]
        return file_path_list

    def clear_driver_pool(self, dont_output=False):
        self._lock.acquire()
        if self.driver_pool:
            for driver in self.driver_pool:
                self.quit_driver(driver, dont_output)
            self.driver_pool = []
        self._lock.release()

    def __del__(self):
        if self.driver_pool:
            self.clear_driver_pool(dont_output=True)


if __name__ == "__main__":
    def parse_baidu_demo(DriverPollObj):
        driver = DriverPollObj.query_driver()
        DriverPollObj.goto(driver, "https://www.baidu.com")
        DriverPollObj.get_download_filepath_list(driver)
        DriverPollObj.out_of_use(driver)
        DriverPollObj.dequeue_driver(driver)

    DriverPollObj = DriverPoll(driver="firefox")
    thd_num = 4
    thd_list = []
    for no in range(1, thd_num+1):
        thd = threading.Thread(target=parse_baidu_demo, args=(DriverPollObj, ))
        thd_list.append(thd)
    for thd in thd_list:
        thd.start()
    for thd in thd_list:
        thd.join()

    DriverPollObj.clear_driver_pool()
