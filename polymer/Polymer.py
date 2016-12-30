
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import MemoryHandler
from billiard import Process, Queue
from Queue import Empty, Full
from copy import deepcopy
import traceback as tb
import logging
import time
import sys

""" Polymer.py - Manage parallel tasks
     Copyright (C) 2015-2016 David Michael Pennington

     This program is free software: you can redistribute it and/or modify
     it under the terms of the GNU General Public License as published by
     the Free Software Foundation, either version 3 of the License, or
     (at your option) any later version.

     This program is distributed in the hope that it will be useful,
     but WITHOUT ANY WARRANTY; without even the implied warranty of
     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
     GNU General Public License for more details.

     You should have received a copy of the GNU General Public License
     along with this program.  If not, see <http://www.gnu.org/licenses/>.

     If you need to contact the author, you can do so by emailing:
     mike [~at~] pennington [/dot\] net
"""

class Worker(object):
    """multiprocessing worker"""
    def __init__(self, w_id, t_q, r_q, default_sleep=0.00001):
        assert isinstance(default_sleep, int) or isinstance(default_sleep, 
            float)
        self.w_id = w_id
        self.cycle_sleep = default_sleep      # How long the worker should sleep
        self.task = None
        try:
            self.ready(t_q, r_q)  # Get the worker ready to do work
        except:
            self.task = None
            tb_str = ''.join(tb.format_exception(*(sys.exc_info())))
            r_q.put({'w_id': self.w_id, 
                'task': self.task,
                'error': tb_str,
                'state': '__ERROR__'})

    def ready(self, t_q, r_q):
        t_msg = {}
        while (t_msg.get('state', '')!='__DIE__'):
            try:
                t_msg = t_q.get(True, self.cycle_sleep)  # Poll blocking
                self.task = t_msg.get('task', '')        # __DIE__ has no task
                if self.task!='':
                    self.task.task_start = time.time()   # Start the timer

                    # Send ACK to the controller who requested work on this task
                    r_q.put({'w_id': self.w_id, 
                        'task': self.task,
                        'state': '__ACK__'})

                    # Update the sleep time with latest recommendations
                    self.cycle_sleep = self.task.worker_loop_delay

                    # Assign the result of task.run() to task.result
                    self.task.result = self.task.run()
                    self.task.task_stop = time.time()  # Seconds since epoch

                    r_q.put({'w_id': self.w_id, 
                        'task': self.task,
                        'state': '__FINISHED__'})  # Ack work finished

                    self.task = None
            except Empty:
                pass
            except Full:
                time.sleep(0.1)
        return

class TaskMgrStats(object):
    def __init__(self, worker_count, log_interval=60):
        self.log_interval = log_interval
        self.stats_start = time.time()
        self.exec_times   = list()    # Archive of all exec times
        self.queue_times  = list()    # Archive of all queue times
        self.worker_count = worker_count

    def reset(self):
        self.stats_start = time.time()
        self.exec_times   = list()
        self.queue_times  = list()

    @property
    def worker_pct_busy(self):
        #return time_worked/total_work_time*100.0
        return time_worked

    @property
    def time_delta(self):
        return (time.time()-self.stats_start)

    @property
    def log_time(self):
        """Return True if it's time to log"""
        if self.time_delta>=self.log_interval:
            return True
        return False

    @property
    def log_message(self):
        """Build a log message and reset the stats"""
        time_delta = deepcopy(self.time_delta)
        total_work_time = self.worker_count*time_delta
        time_worked = sum(self.exec_times)
        pct_busy = time_worked/total_work_time*100.0

        min_task_time = min(self.exec_times)
        avg_task_time = sum(self.exec_times)/len(self.exec_times)
        max_task_time = max(self.exec_times)

        min_queue_time = min(self.queue_times)
        avg_queue_time = sum(self.queue_times)/len(self.queue_times)
        max_queue_time = max(self.queue_times)

        time_delta = self.time_delta
        total_tasks   = len(self.exec_times)
        avg_task_rate = total_tasks/time_delta

        self.reset()

        task_msg = """Ran {0} tasks, {1} tasks/s; {2} workers {3}% busy""".format(total_tasks, round(avg_task_rate, 1), self.worker_count, round(pct_busy, 1))
        task_mam  = """     Task run times: {0}/{1}/{2} (min/avg/max)""".format(round(min_task_time, 3), round(avg_task_time, 3), round(max_task_time, 3))
        queue_mam = """     Time in queue: {0}/{1}/{2} (min/avg/max)""".format(round(min_queue_time, 6), round(avg_queue_time, 6), round(max_queue_time, 6))

        return """{0}\n{1}\n{2}""".format(task_msg, task_mam, queue_mam)

class TaskMgr(object):
    """Manage tasks to and from workers; maybe one day use zmq instead of 
    multiprocessing.Queue"""
    # http://www.jeffknupp.com/blog/2014/02/11/a-celerylike-python-task-queue-in-55-lines-of-code/
    def __init__(self, work_todo=None, log_level=3, log_stdout=True, 
        log_path='taskmgr.log', queue=None, hot_loop=False, 
        worker_count=5, log_interval=60, worker_cycle_sleep=0.000001):
        if work_todo is None:
            work_todo = list()
        assert isinstance(work_todo, list), "Please add work in a python list"
        self.work_todo = work_todo     # List with work to do

        self.worker_count = worker_count
        self.worker_cycle_sleep = worker_cycle_sleep
        self.log_level = log_level     # 0: off, 1: errors, 2: info, 3: debug
        self.log_stdout = log_stdout
        self.log_path = log_path
        self.log_interval = log_interval

        self.t_q   = Queue()           # workers listen to t_q (task queue)
        self.r_q   = Queue()           # results queue
        self.assignments = dict()      # key: w_id, value: worker Process objs
        self.results = dict()
        self.configure_logging()
        self.hot_loop = hot_loop

        if hot_loop:
            assert isinstance(queue, ControllerQueue)
            self.controller = queue
            self.supervise()          # hot_loops automatically supervise()

    def configure_logging(self):
        if self.log_level:
            self.log = logging.getLogger(__name__)
            fmtstr = '%(asctime)s.%(msecs).03d %(levelname)s %(message)s'
            format = logging.Formatter(fmt=fmtstr, datefmt='%Y-%m-%d %H:%M:%S')
            self.log.setLevel(logging.DEBUG)

            if self.log_path:
                ## Rotate the logfile every day...
                rotating_file = TimedRotatingFileHandler(self.log_path,
                    when="D", interval=1, backupCount=5)
                rotating_file.setFormatter(format)
                memory_log = logging.handlers.MemoryHandler(1024*10, 
                    logging.DEBUG, rotating_file)
                self.log.addHandler(memory_log)
            if self.log_stdout:
                console = logging.StreamHandler()
                console.setFormatter(format)
                self.log.addHandler(console)

            if not bool(self.log_path) and (not self.log_stdout):
                self.log_level = 0

    def supervise(self):
        """If not in a hot_loop, call supervise() to start the tasks"""
        retval = set([])
        stats = TaskMgrStats(worker_count=self.worker_count, 
            log_interval=self.log_interval)

        hot_loop = self.hot_loop
        if self.log_level>=2:
            logmsg = "TaskMgr.supervise() started {0} workers".format(self.worker_count)
            self.log.info(logmsg)
        self.workers = self.spawn_workers()

        ## Add work
        if not hot_loop:
            for task in self.work_todo:
                self.queue_task(task)

        finished = False
        while not finished:
            try:
                if hot_loop:
                    # Calculate the adaptive loop delay
                    delay = self.calc_wait_time(stats.exec_times)
                    self.queue_tasks_from_controller(delay=delay)  # queue tasks
                    time.sleep(delay)


                r_msg = self.r_q.get_nowait()     # __ACK__ or __FINISHED__
                task = r_msg.get('task')
                w_id = r_msg.get('w_id')
                state = r_msg.get('state', '')
                if state=='__ACK__':
                    self.assignments[w_id] = task
                    self.work_todo.remove(task)
                    if self.log_level>=3:
                        self.log.debug("r_msg: {0}".format(r_msg))
                    if self.log_level>=3:
                        self.log.debug("w_id={0} received task={1}".format(w_id,
                            task))
                elif state=='__FINISHED__':
                    now = time.time()
                    task_exec_time = task.task_stop - task.task_start
                    task_queue_time = now - task.queue_time - task_exec_time
                    stats.exec_times.append(task_exec_time)
                    stats.queue_times.append(task_queue_time)

                    if self.log_level>=3:
                        self.log.debug("r_msg: {0}".format(r_msg))

                    if not hot_loop:
                        retval.add(task)  # Add result to retval
                        self.assignments.pop(w_id)  # Delete the key
                        finished = self.is_finished()
                    else:
                        self.controller.to_q.put(task)  # Send to the controller
                        self.assignments.pop(w_id)  # Delete the key


                elif state=='__ERROR__':
                    if self.log_level>=1:
                        self.log.error("r_msg: {0}".format(r_msg))
                        self.log.error(''.join(r_msg.get('error')))
            except Empty:
                state = '__EMPTY__'

            if stats.log_time:
                if self.log_level>=2:
                    self.log.info(stats.log_message)

            # Adaptive loop delay
            delay = self.calc_wait_time(stats.exec_times)
            time.sleep(delay)

            self.respawn_dead_workers()

        if not hot_loop:
            self.kill_workers()
            for w_id, p in self.workers.items():
                p.join()
            return retval

    def calc_wait_time(self, exec_times):
        num_samples = float(len(exec_times))
        if num_samples>0.0:
            queue_size = max(self.r_q.qsize(),1.0)+max(self.t_q.qsize(),1.0)
            min_task_time = min(exec_times)
            wait_time = min_task_time/queue_size
        else:
            wait_time = 0.00001   # 10us delay to avoid worker / r_q race 
        return wait_time

    def queue_tasks_from_controller(self, delay=0.0):
        finished = False
        while not finished:
            try:
                ## Hot loops will queue a list of tasks...
                tasklist = self.controller.from_q.get_nowait()
                for task in tasklist:
                    if delay>0.0:
                        task.worker_loop_delay = delay
                    self.work_todo.append(task)
                    self.queue_task(task)
            except Empty:
                # Poll until empty
                finished = True
            except (Exception) as e:
                tb.print_exc()

    def queue_task(self, task):
        task.queue_time = time.time()   # Record the queueing time
        self.t_q.put({'task': task})

    def is_finished(self):
        if (len(self.work_todo)==0) and (len(self.assignments.keys())==0):
            return True
        return False

    def kill_workers(self):
        stop = {'state': '__DIE__'}
        [self.t_q.put(stop) for x in xrange(0, self.worker_count)]

    def respawn_dead_workers(self):
        for w_id, p in self.workers.items():
            if not p.is_alive():
                # Queue the task for another worker
                task = self.assignments.get(w_id, {})
                if task!={}:
                    del self.assignments[w_id]
                    self.work_todo.append(task)
                    self.queue_task(task)
                if self.log_level>=2:
                    self.log.info("Respawning w_id={0}".format(w_id))
                self.workers[w_id] = Process(target=Worker, 
                    args=(w_id, self.t_q, self.r_q, self.worker_cycle_sleep))
                self.workers[w_id].start()

    def spawn_workers(self):
        workers = dict()
        for w_id in xrange(0, self.worker_count):
            workers[w_id] = Process(target=Worker, 
                name="Polymer.py Worker {0}".format(w_id), 
                args=(w_id, self.t_q, self.r_q, self.worker_cycle_sleep))
            workers[w_id].start()
        return workers

class ControllerQueue(object):
    """A set of queues to manage a continuous hot TaskMgr work loop"""
    def __init__(self):
        ## to and from are with respect to the (client) controller object
        self.to_q   = Queue()  # sent to the controller from TaskMgr
        self.from_q = Queue()  # sent from the controller to TaskMgr
