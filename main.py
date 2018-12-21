# coding=utf-8
from driver_pool import DriverPoll
import threading


def parse_baidu_demo(DriverPollObj):
    driver = DriverPollObj.query_driver()
    DriverPollObj.goto(driver, "https://www.baidu.com")
    DriverPollObj.get_download_filepath_list(driver)
    DriverPollObj.out_of_use(driver)
    DriverPollObj.dequeue_driver(driver)


DriverPollObj = DriverPoll(driver="firefox")
thd_num = 4
thd_list = []
for no in range(1, thd_num + 1):
    thd = threading.Thread(target=parse_baidu_demo, args=(DriverPollObj,))
    thd_list.append(thd)
for thd in thd_list:
    thd.start()
for thd in thd_list:
    thd.join()

DriverPollObj.clear_driver_pool()