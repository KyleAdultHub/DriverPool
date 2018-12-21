# -*- coding: utf-8 -*-
import logging
import os
from logging.handlers import TimedRotatingFileHandler


class Logger(object):
    def __init__(self, name, log_folder=""):
        self.log_folder = log_folder
        self.name = name
        self.logger = logging.getLogger(name)
        self.formatter = self.get_formatter()
        self.file_holder = self.get_file_handler() if self.log_folder else None
        self.stream_handler = self.get_stream_handler()
        self.setting_logger()

    def check_file_path(self):
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)

    @staticmethod
    def get_formatter():
        return logging.Formatter('%(asctime)s : %(name)s : %(levelname)s :[%(lineno)d]: %(message)s')

    @staticmethod
    def get_stream_handler():
        return logging.StreamHandler()

    def get_file_handler(self):
        self.check_file_path()
        file_name = self.log_folder + "/{}_pid_{}.log".format(self.name, os.getpid())
        file_handler = TimedRotatingFileHandler(filename=file_name, when="D", interval=1, backupCount=30)
        file_handler.suffix = "%Y-%m-%d"
        return file_handler

    def setting_logger(self):
        self.logger.setLevel(logging.DEBUG)
        if self.file_holder:
            self.file_holder.setLevel(logging.INFO)
            self.file_holder.setFormatter(self.formatter)
        self.stream_handler.setLevel(logging.DEBUG)
        self.stream_handler.setFormatter(self.formatter)
        if self.file_holder:
            self.logger.addHandler(self.file_holder)
        self.logger.addHandler(self.stream_handler)

    def __del__(self):
        if self.file_holder:
            self.file_holder.close()
        self.stream_handler.close()