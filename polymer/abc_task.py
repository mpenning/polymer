from abc import ABCMeta
import time

class BaseTask(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.addr = None           # String IP Address
        self.task_stop    = 0.0    # Data timestamp, float seconds since epoch
        self.task_start   = 0.0    # When task was run by a worker
        self.queue_time   = 0.0    # When TaskMgr queued task for a worker

        self.worker_loop_delay = 0.00001   # Default 10us worker sleep delay 

    def run(self):
        raise NotImplementedError
