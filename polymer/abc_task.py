from __future__ import absolute_import
from abc import ABCMeta, abstractmethod


class BaseTask(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.addr = None  # String IP Address
        self.task_stop = 0.0  # Data timestamp, float seconds since epoch
        self.task_start = 0.0  # When task was run by a worker
        self.queue_time = 0.0  # When TaskMgr queued task for a worker

        self.worker_loop_delay = 0.00001  # Default 10us worker sleep delay

    def __hash__(self):
        return id(self)

    @abstractmethod
    def __repr__(self):
        raise NotImplementedError

    @abstractmethod
    def __eq__(self):
        """Define how tasks are uniquely identified"""
        raise NotImplementedError(
            "Be sure to use isinstance() for all __eq__() checks.  See README"
        )

    @abstractmethod
    def run(self):
        """Define what should be done"""
        raise NotImplementedError
