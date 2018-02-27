import datetime
from abc import ABCMeta, abstractmethod


class ExecutionHandler:

    __metaclass__ = ABCMeta

    @abstractmethod
    def execute_order(self, event):
        raise NotImplementedError


class IBExecutionHandler(ExecutionHandler):
    def __init__(self):
        pass

    def execute_order(self, event):
        return
