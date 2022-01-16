from __future__ import absolute_import

from loguru import logger

global PACKAGE_NAME
PACKAGE_NAME = "Polymer"

import multiprocessing.queues as mpq
from multiprocessing import Process
import multiprocessing
from datetime import datetime
from copy import deepcopy
from hashlib import md5
import traceback as tb
import time
import sys
import os

import _pickle as pickle  # Python3

# This works in Python3.x...
from queue import Empty, Full

from colorama import init as color_init
from colorama import Fore, Style

""" Polymer.py - Manage parallel tasks
     Copyright (C) 2021-2022 David Michael Pennington
     Copyright (C) 2020-2021 David Michael Pennington at Cisco Systems
     Copyright (C) 2019      David Michael Pennington at ThousandEyes
     Copyright (C) 2015-2019 David Michael Pennington at Samsung Data Services

                    GNU GENERAL PUBLIC LICENSE
                       Version 3, 29 June 2007

 Copyright (C) 2007 Free Software Foundation, Inc. <http://fsf.org/>
 Everyone is permitted to copy and distribute verbatim copies
 of this license document, but changing it is not allowed.
"""


class SharedCounter(object):
    """A synchronized shared counter.
    The locking done by multiprocessing.Value ensures that only a single
    process or thread may read or write the in-memory ctypes object. However,
    in order to do n += 1, Python performs a read followed by a write, so a
    second process may read the old value before the new one is written by the
    first process. The solution is to use a multiprocessing.Lock to guarantee
    the atomicity of the modifications to Value.

    This class comes almost entirely from Eli Bendersky's blog:
    http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing/
    """

    def __init__(self, n=0):
        self.count = multiprocessing.Value("i", n)

    def increment(self, n=1):
        """ Increment the counter by n (default = 1) """
        with self.count.get_lock():
            self.count.value += n

    @property
    def value(self):
        """ Return the value of the counter """
        return self.count.value


################################################################################
#
# py3_mp_queue() is heavily based on the following github repo's commit...
# http://github.com/vterron/lemon/commit/9ca6b4b1212228dbd4f69b88aaf88b12952d7d6f
# Code license is GPLv3 according to github.com/vterron/lemon/setup.py
#
################################################################################
class py3_mp_queue(mpq.Queue):
    """A portable implementation of multiprocessing.Queue.
    Because of multithreading / multiprocessing semantics, Queue.qsize() may
    raise the NotImplementedError exception on Unix platforms like Mac OS X
    where sem_getvalue() is not implemented. This subclass addresses this
    problem by using a synchronized shared counter (initialized to zero) and
    increasing / decreasing its value every time the put() and get() methods
    are called, respectively. This not only prevents NotImplementedError from
    being raised, but also allows us to implement a reliable version of both
    qsize() and empty().
    """

    # NOTE This is the new implementation based on:
    #    https://github.com/vterron/lemon/blob/master/util/queue.py

    def __init__(self, *args, **kwargs):
        # Use ctx argument if using Python3.4+
        super(py3_mp_queue, self).__init__(
            *args, ctx=multiprocessing.get_context("spawn"), **kwargs
        )

        if not sys.platform == "darwin":
            self.size = SharedCounter(0)

    def put(self, *args, **kwargs):
        # Infinite recursion possible if we don't use super() here
        try:
            super(py3_mp_queue, self).put(*args, **kwargs)

        except Full:
            pass

        if not sys.platform == "darwin":
            self.size.increment(1)

    def get(self, *args, **kwargs):
        if not sys.platform == "darwin":
            self.size.increment(-1)
        try:
            item = super(py3_mp_queue, self).get(*args, **kwargs)
            return item

        except Empty:
            return {}

    def qsize(self):
        """ Reliable implementation of multiprocessing.Queue.qsize() """
        if not sys.platform == "darwin":
            return self.size.value
        else:
            raise NotImplementedError

    def empty(self):
        """ Reliable implementation of multiprocessing.Queue.empty() """
        if not sys.platform == "darwin":
            return not self.qsize()
        else:
            raise NotImplementedError

    def clear(self):
        """ Remove all elements from the Queue. """
        while not self.empty():
            self.get()


class Worker(object):
    """multiprocessing worker"""

    def __init__(self, w_id, todo_q, done_q, default_sleep=0.00001):
        assert isinstance(default_sleep, int) or isinstance(default_sleep, float)
        color_init()
        self.w_id = w_id
        self.cycle_sleep = default_sleep  # How long the worker should sleep
        self.task = None
        self.done_q = done_q

        try:
            self.message_loop(todo_q, done_q)  # do work
        except Exception:
            if self.task is not None:
                self.task.task_stop = time.time()  # Seconds since epoch

            ## Format and return the error
            tb_str = "".join(tb.format_exception(*(sys.exc_info())))
            self.done_q_send(
                {
                    "w_id": self.w_id,
                    "task": self.task,
                    "error": tb_str,
                    "state": "__ERROR__",
                }
            )

            # self.cycle_sleep = self.task.worker_loop_delay
            # time.sleep(self.cycle_sleep)
            self.task = None

    def done_q_send(self, msg_dict):
        """Send message dicts through done_q, and throw explicit errors for
        pickle problems"""

        # Check whether msg_dict can be pickled...
        no_pickle_keys = self.invalid_dict_pickle_keys(msg_dict)

        if no_pickle_keys == []:
            self.done_q.put(msg_dict)

        else:
            ## Explicit pickle error handling
            hash_func = md5()
            hash_func.update(str(msg_dict))
            dict_hash = str(hash_func.hexdigest())[-7:]  # Last 7 digits of hash
            linesep = os.linesep
            sys.stderr.write(
                "{0} {1}done_q_send({2}) Can't pickle this dict:{3} '''{7}{4}   {5}{7}{6}''' {7}".format(
                    datetime.now(),
                    Style.BRIGHT,
                    dict_hash,
                    Style.RESET_ALL,
                    Fore.MAGENTA,
                    str(msg_dict),
                    Style.RESET_ALL,
                    linesep,
                )
            )
            sys.stderr.write(
                "{0}          {1}Pickling problems often come from open or hung TCP sockets{2}{3}".format(
                    datetime.now(),
                    Style.BRIGHT,
                    Style.RESET_ALL,
                    linesep,
                )
            )

            ## Verbose list of the offending key(s) / object attrs
            ## Send all output to stderr...
            err_frag1 = (
                Style.BRIGHT
                + "    done_q_send({0}) Offending dict keys:".format(str(dict_hash))
                + Style.RESET_ALL
            )
            err_frag2 = Fore.YELLOW + " {0}".format(no_pickle_keys) + Style.RESET_ALL
            err_frag3 = "{0}".format(linesep)
            sys.stderr.write(err_frag1 + err_frag2 + err_frag3)
            for key in sorted(no_pickle_keys):
                sys.stderr.write(
                    "      msg_dict['{0}']: {1}'{2}'{3}{4}".format(
                        key,
                        Fore.MAGENTA,
                        repr(msg_dict.get(key)),
                        Style.RESET_ALL,
                        linesep,
                    )
                )
                if isinstance(msg_dict.get(key), object):
                    thisobj = msg_dict.get(key)
                    no_pickle_attrs = self.invalid_obj_pickle_attrs(thisobj)
                    err_frag1 = (
                        Style.BRIGHT
                        + "      done_q_send({0}) Offending attrs:".format(dict_hash)
                        + Style.RESET_ALL
                    )
                    err_frag2 = (
                        Fore.YELLOW + " {0}".format(no_pickle_attrs) + Style.RESET_ALL
                    )
                    err_frag3 = "{0}".format(linesep)
                    sys.stderr.write(err_frag1 + err_frag2 + err_frag3)
                    for attr in no_pickle_attrs:
                        sys.stderr.write(
                            "        msg_dict['{0}'].{1}: {2}'{3}'{4}{5}".format(
                                key,
                                attr,
                                Fore.RED,
                                repr(getattr(thisobj, attr)),
                                Style.RESET_ALL,
                                linesep,
                            )
                        )

            sys.stderr.write(
                "    {0}done_q_send({1}) keys (no problems):{2}{3}".format(
                    Style.BRIGHT, dict_hash, Style.RESET_ALL, linesep
                )
            )
            for key in sorted(set(msg_dict.keys()).difference(no_pickle_keys)):
                sys.stderr.write(
                    "      msg_dict['{0}']: {1}{2}{3}{4}".format(
                        key,
                        Fore.GREEN,
                        repr(msg_dict.get(key)),
                        Style.RESET_ALL,
                        linesep,
                    )
                )

    def invalid_dict_pickle_keys(self, msg_dict):
        """Return a list of keys that can't be pickled.  Return [] if
        there are no pickling problems with the values associated with the
        keys.  Return the list of keys, if there are problems."""
        no_pickle_keys = list()
        for key, val in msg_dict.items():

            try:
                pickle.dumps(key)
                pickle.dumps(val)
            except TypeError:
                no_pickle_keys.append(key)  # This key has an unpicklable value
            except pickle.PicklingError:
                no_pickle_keys.append(key)  # This key has an unpicklable value
            except pickle.UnpickleableError:
                no_pickle_keys.append(key)  # This key has an unpicklable value

        return no_pickle_keys

    def invalid_obj_pickle_attrs(self, thisobj):
        no_pickle_attrs = list()
        for attr, val in vars(thisobj).items():
            try:
                pickle.dumps(getattr(thisobj, attr))
            except TypeError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
            except pickle.PicklingError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
            except pickle.UnpickleableError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
        return no_pickle_attrs

    def message_loop(self, todo_q, done_q):
        """Loop through messages and execute tasks"""
        t_msg = {}
        while t_msg.get("state", "") != "__DIE__":
            try:
                t_msg = todo_q.get(True, self.cycle_sleep)  # Poll blocking
                self.task = t_msg.get("task", "")  # __DIE__ has no task
                if self.task != "":

                    self.task.task_start = time.time()  # Start the timer
                    # Send ACK to the controller who requested work on this task
                    self.done_q_send(
                        {"w_id": self.w_id, "task": self.task, "state": "__ACK__"}
                    )

                    # Update the sleep time with latest recommendations
                    self.cycle_sleep = self.task.worker_loop_delay

                    # Assign the result of task.run() to task.result
                    self.task.result = self.task.run()
                    self.task.task_stop = time.time()  # Seconds since epoch

                    self.done_q_send(
                        {"w_id": self.w_id, "task": self.task, "state": "__FINISHED__"}
                    )  # Ack work finished

                    self.task = None
            except Empty:
                pass
            except Full:
                time.sleep(0.1)
            ## Disable extraneous error handling...
            except Exception:
                if self.task is not None:
                    self.task.task_stop = time.time()  # Seconds since epoch
                # Handle all other errors here...
                tb_str = "".join(tb.format_exception(*(sys.exc_info())))
                self.done_q_send(
                    {
                        "w_id": self.w_id,
                        "task": self.task,
                        "error": tb_str,
                        "state": "__ERROR__",
                    }
                )

        return


class TaskMgrStats(object):
    def __init__(self, worker_count, log_interval=60, hot_loop=False):
        self.log_interval = log_interval
        self.stats_start = time.time()
        self.exec_times = list()  # Archive of all exec times
        self.queue_times = list()  # Archive of all queue times
        self.worker_count = worker_count
        self.hot_loop = hot_loop

    def reset(self):
        self.stats_start = time.time()
        self.exec_times = list()
        self.queue_times = list()

    @property
    def worker_pct_busy(self):
        # return time_worked/total_work_time*100.0
        return time_worked

    @property
    def time_delta(self):
        return time.time() - self.stats_start

    @property
    def log_time(self):
        """Return True if it's time to log"""
        if self.hot_loop and self.time_delta >= self.log_interval:
            return True
        return False

    # FIXME - stopped here...
    @property
    def log_message(self):
        """Build a log message and reset the stats"""
        time_delta = deepcopy(self.time_delta)
        total_work_time = self.worker_count * time_delta
        time_worked = sum(self.exec_times)
        pct_busy = time_worked / total_work_time * 100.0

        min_task_time = min(self.exec_times)
        avg_task_time = sum(self.exec_times) / len(self.exec_times)
        max_task_time = max(self.exec_times)

        min_queue_time = min(self.queue_times)
        avg_queue_time = sum(self.queue_times) / len(self.queue_times)
        max_queue_time = max(self.queue_times)

        time_delta = self.time_delta
        total_tasks = len(self.exec_times)
        avg_task_rate = total_tasks / time_delta

        self.reset()

        task_msg = """Ran {0} tasks, {1} tasks/s; {2} workers {3}% busy""".format(
            total_tasks, round(avg_task_rate, 1), self.worker_count, round(pct_busy, 1)
        )
        task_mam = """     Task run times: {0}/{1}/{2} (min/avg/max)""".format(
            round(min_task_time, 3), round(avg_task_time, 3), round(max_task_time, 3)
        )
        queue_mam = """     Time in queue: {0}/{1}/{2} (min/avg/max)""".format(
            round(min_queue_time, 6), round(avg_queue_time, 6), round(max_queue_time, 6)
        )

        return """{0}\n{1}\n{2}""".format(task_msg, task_mam, queue_mam)


class TaskMgr(object):
    """Manage tasks to and from workers; maybe one day use zmq instead of
    multiprocessing.Queue"""

    # http://www.jeffknupp.com/blog/2014/02/11/a-celerylike-python-task-queue-in-55-lines-of-code/
    def __init__(
        self,
        work_todo=None,
        log_level=3,
        log_stdout=True,
        log_path="taskmgr.log",
        queue=None,
        hot_loop=False,
        worker_count=5,
        log_interval=60,
        worker_cycle_sleep=0.000001,
        resubmit_on_error=False,
    ):
        if work_todo is None:
            work_todo = list()
        assert isinstance(work_todo, list), "Please add work in a python list"
        self.work_todo = work_todo  # List with work to do

        self.worker_count = worker_count
        self.worker_cycle_sleep = worker_cycle_sleep
        self.log_level = log_level  # 0: off, 1: errors, 2: info, 3: debug
        self.log_stdout = log_stdout
        self.log_path = log_path
        self.log_interval = log_interval
        self.resubmit_on_error = resubmit_on_error

        # By default, Python3's multiprocessing.Queue doesn't implement qsize()
        # NOTE:  OSX doesn't implement queue.qsize(), py3_mp_queue is a
        #     workaround
        # AttributeError: 'module' object has no attribute 'get_context'
        self.todo_q = py3_mp_queue()  # workers listen to todo_q (task queue)
        self.done_q = py3_mp_queue()  # results queue

        self.worker_assignments = dict()  # key: w_id, value: worker Process objs
        self.results = dict()
        self.configure_logging()
        self.hot_loop = hot_loop
        self.retval = set([])

        color_init()

        if hot_loop:
            assert isinstance(queue, ControllerQueue)
            self.controller = queue
            self.supervise()  # hot_loops automatically supervise()

    def configure_logging(self):
        logger.disable(PACKAGE_NAME)
        if self.log_level:

            if self.log_path:
                logger.add(sink=self.log_path)
            if self.log_stdout:
                logger.add(sink=sys.stdout)

            if not bool(self.log_path) and (not self.log_stdout):
                self.log_level = 0
            logger.enable(PACKAGE_NAME)

    def supervise(self):
        """If not in a hot_loop, call supervise() to start the tasks"""
        self.retval = set([])
        stats = TaskMgrStats(
            worker_count=self.worker_count,
            log_interval=self.log_interval,
            hot_loop=self.hot_loop,
        )

        hot_loop = self.hot_loop
        if self.log_level >= 2:
            logmsg = "TaskMgr.supervise() started {0} workers".format(self.worker_count)
            logger.info(logmsg)
        self.workers = self.spawn_workers()

        ## Add work
        self.num_tasks = 0
        if not hot_loop:
            if self.log_level >= 2:
                logmsg = "TaskMgr.supervise() received {0} tasks".format(
                    len(self.work_todo)
                )
                logger.info(logmsg)
            for task in self.work_todo:
                self.num_tasks += 1
                if self.log_level >= 2:
                    logmsg = "TaskMgr.supervise() queued task: {0}".format(task)
                    logger.info(logmsg)
                self.queue_task(task)

        finished = False
        while not finished:
            try:
                if hot_loop:
                    # Calculate the adaptive loop delay
                    delay = self.calc_wait_time(stats.exec_times)
                    self.queue_tasks_from_controller(delay=delay)  # queue tasks
                    time.sleep(delay)

                r_msg = self.done_q.get_nowait()  # __ACK__ or __FINISHED__
                task = r_msg.get("task")
                w_id = r_msg.get("w_id")
                state = r_msg.get("state", "")
                if state == "__ACK__":
                    self.worker_assignments[w_id] = task
                    self.work_todo.remove(task)
                    if self.log_level >= 3:
                        logger.debug("r_msg: {0}".format(r_msg))
                    if self.log_level >= 3:
                        logger.debug("w_id={0} received task={1}".format(w_id, task))
                elif state == "__FINISHED__":
                    now = time.time()
                    task_exec_time = task.task_stop - task.task_start
                    task_queue_time = now - task.queue_time - task_exec_time
                    stats.exec_times.append(task_exec_time)
                    stats.queue_times.append(task_queue_time)

                    if self.log_level >= 1:
                        logger.info(
                            "TaskMgr.work_todo: {0} tasks left".format(
                                len(self.work_todo)
                            )
                        )
                    if self.log_level >= 3:
                        logger.debug("TaskMgr.work_todo: {0}".format(self.work_todo))
                        logger.debug("r_msg: {0}".format(r_msg))

                    if not hot_loop:
                        self.retval.add(task)  # Add result to retval
                        self.worker_assignments.pop(w_id)  # Delete the key
                        finished = self.is_finished()
                    else:
                        self.controller.from_taskmgr_q.put(
                            task
                        )  # Send to the controller
                        self.worker_assignments.pop(w_id)  # Delete the key

                elif state == "__ERROR__":

                    now = time.time()
                    task_exec_time = task.task_stop - task.task_start
                    task_queue_time = now - task.queue_time - task_exec_time
                    stats.exec_times.append(task_exec_time)
                    stats.queue_times.append(task_queue_time)

                    if self.log_level >= 1:
                        logger.error("r_msg: {0}".format(r_msg))
                        logger.error("".join(r_msg.get("error")))
                        logger.debug(
                            "TaskMgr.work_todo: {0} tasks left".format(
                                len(self.work_todo)
                            )
                        )
                    if self.log_level >= 3:
                        logger.debug("TaskMgr.work_todo: {0}".format(self.work_todo))

                    if not hot_loop:
                        if not self.resubmit_on_error:
                            # If task is in work_todo, delete it
                            for tt in self.work_todo:
                                if tt == task:
                                    self.work_todo.remove(task)  # Remove task...

                            try:
                                # Delete the worker assignment...
                                self.worker_assignments.pop(w_id)
                            except Exception:
                                pass
                            self.retval.add(task)  # Add result to retval

                    self.respawn_dead_workers()

            except Empty:
                state = "__EMPTY__"

            except Exception as ee:
                tb_str = "".join(tb.format_exception(*(sys.exc_info())))
                print("ERROR:")
                print(ee, tb_str)

            if stats.log_time:
                if self.log_level >= 0:
                    logger.info(stats.log_message)

            # Adaptive loop delay unless on Mac OSX... OSX delay is constant...
            delay = self.calc_wait_time(stats.exec_times)
            time.sleep(delay)

            self.respawn_dead_workers()
            finished = self.is_finished()

        if not hot_loop:
            self.kill_workers()
            for w_id, p in self.workers.items():
                p.join()

            ## Log a final stats summary...
            if self.log_level > 0:
                logger.info(stats.log_message)
            return self.retval

    def calc_wait_time(self, exec_times):
        num_samples = float(len(exec_times))

        # Work around Mac OSX problematic queue.qsize() implementation...
        if sys.platform == "darwin":
            wait_time = 0.00001  # 10us delay to avoid worker / done_q race

        elif num_samples > 0.0:
            # NOTE:  OSX doesn't implement queue.qsize(), I worked around the
            # problem
            queue_size = max(self.done_q.qsize(), 1.0) + max(self.todo_q.qsize(), 1.0)
            min_task_time = min(exec_times)
            wait_time = min_task_time / queue_size

        else:
            wait_time = 0.00001  # 10us delay to avoid worker / done_q race

        return wait_time

    def queue_tasks_from_controller(self, delay=0.0):
        finished = False
        while not finished:
            try:
                ## Hot loops will queue a list of tasks...
                tasklist = self.controller.to_taskmgr_q.get_nowait()
                for task in tasklist:
                    if delay > 0.0:
                        task.worker_loop_delay = delay
                    self.work_todo.append(task)
                    self.queue_task(task)
            except Empty:
                # Poll until empty
                finished = True
            # except (Exception) as e:
            #    tb.print_exc()

    def queue_task(self, task):
        task.queue_time = time.time()  # Record the queueing time
        self.todo_q_send({"task": task})

    def is_finished(self):
        if (len(self.work_todo) == 0) and (len(self.worker_assignments.keys()) == 0):
            return True
        elif not self.hot_loop and (len(self.retval)) == self.num_tasks:
            # We need this exit condition due to __ERROR__ race conditions...
            return True
        return False

    def kill_workers(self):
        stop = {"state": "__DIE__"}
        [self.todo_q_send(stop) for x in range(0, self.worker_count)]

    def respawn_dead_workers(self):
        """Respawn workers / tasks upon crash"""
        for w_id, p in self.workers.items():
            if not p.is_alive():
                # Queue the task for another worker, if required...
                if self.log_level >= 2:
                    logger.info("Worker w_id {0} died".format(w_id))
                task = self.worker_assignments.get(w_id, {})
                if self.log_level >= 2 and task != {}:
                    logger.info(
                        "Dead worker w_id {0} was assigned task - {1}".format(
                            w_id, task
                        )
                    )
                error_suffix = ""
                if task != {}:
                    del self.worker_assignments[w_id]
                    if self.resubmit_on_error or self.hot_loop:
                        self.work_todo.append(task)
                        self.queue_task(task)
                        if self.log_level >= 2:
                            logger.info("Resubmitting task - {0}".format(task))
                        error_suffix = " with task={1}".format(task)
                if self.log_level >= 1:
                    logger.debug(
                        "TaskMgr.work_todo: {0} tasks left".format(len(self.work_todo))
                    )
                if self.log_level >= 2:
                    logger.info(
                        "Respawning worker - w_id={0}{1}".format(w_id, error_suffix)
                    )
                self.workers[w_id] = Process(
                    target=Worker,
                    args=(w_id, self.todo_q, self.done_q, self.worker_cycle_sleep),
                )
                self.workers[w_id].daemon = True
                self.workers[w_id].start()

    def spawn_workers(self):
        workers = dict()
        for w_id in range(0, self.worker_count):
            workers[w_id] = Process(
                target=Worker,
                name="Polymer.py Worker {0}".format(w_id),
                args=(w_id, self.todo_q, self.done_q, self.worker_cycle_sleep),
            )
            workers[w_id].daemon = True
            workers[w_id].start()
        return workers

    def todo_q_send(self, msg_dict):

        # Check whether msg_dict can be pickled...
        no_pickle_keys = self.invalid_dict_pickle_keys(msg_dict)

        if no_pickle_keys == []:
            self.todo_q.put(msg_dict)
        else:
            ## Explicit pickle error handling
            hash_func = md5()
            hash_func.update(str(msg_dict))
            dict_hash = str(hash_func.hexdigest())[-7:]  # Last 7 digits of hash
            linesep = os.linesep
            sys.stderr.write(
                "{0} {1}todo_q_send({2}) Can't pickle this dict:{3} '''{7}{4}   {5}{7}{6}''' {7}".format(
                    datetime.now(),
                    Style.BRIGHT,
                    dict_hash,
                    Style.RESET_ALL,
                    Fore.MAGENTA,
                    msg_dict,
                    Style.RESET_ALL,
                    linesep,
                )
            )

            ## Verbose list of the offending key(s) / object attrs
            ## Send all output to stderr...
            err_frag1 = (
                Style.BRIGHT
                + "    todo_q_send({0}) Offending dict keys:".format(dict_hash)
                + Style.RESET_ALL
            )
            err_frag2 = Fore.YELLOW + " {0}".format(no_pickle_keys) + Style.RESET_ALL
            err_frag3 = "{0}".format(linesep)
            sys.stderr.write(err_frag1 + err_frag2 + err_frag3)
            for key in sorted(no_pickle_keys):
                sys.stderr.write(
                    "      msg_dict['{0}']: {1}'{2}'{3}{4}".format(
                        key,
                        Fore.MAGENTA,
                        repr(msg_dict.get(key)),
                        Style.RESET_ALL,
                        linesep,
                    )
                )
                if isinstance(msg_dict.get(key), object):
                    thisobj = msg_dict.get(key)
                    no_pickle_attrs = self.invalid_obj_pickle_attrs(thisobj)
                    err_frag1 = (
                        Style.BRIGHT
                        + "      todo_q_send({0}) Offending attrs:".format(dict_hash)
                        + Style.RESET_ALL
                    )
                    err_frag2 = (
                        Fore.YELLOW + " {0}".format(no_pickle_attrs) + Style.RESET_ALL
                    )
                    err_frag3 = "{0}".format(linesep)
                    sys.stderr.write(err_frag1 + err_frag2 + err_frag3)
                    for attr in no_pickle_attrs:
                        sys.stderr.write(
                            "        msg_dict['{0}'].{1}: {2}'{3}'{4}{5}".format(
                                key,
                                attr,
                                Fore.RED,
                                repr(getattr(thisobj, attr)),
                                Style.RESET_ALL,
                                linesep,
                            )
                        )

            sys.stderr.write(
                "    {0}todo_q_send({1}) keys (no problems):{2}{3}".format(
                    Style.BRIGHT, dict_hash, Style.RESET_ALL, linesep
                )
            )
            for key in sorted(set(msg_dict.keys()).difference(no_pickle_keys)):
                sys.stderr.write(
                    "      msg_dict['{0}']: {1}{2}{3}{4}".format(
                        key,
                        Fore.GREEN,
                        repr(msg_dict.get(key)),
                        Style.RESET_ALL,
                        linesep,
                    )
                )

    def invalid_dict_pickle_keys(self, msg_dict):
        """Return a list of keys that can't be pickled.  Return [] if
        there are no pickling problems with the values associated with the
        keys.  Return the list of keys, if there are problems."""
        no_pickle_keys = list()
        for key, val in msg_dict.items():
            try:
                pickle.dumps(key)
                pickle.dumps(val)
            except TypeError:
                no_pickle_keys.append(key)  # This key has an unpicklable value
            except pickle.PicklingError:
                no_pickle_keys.append(key)  # This key has an unpicklable value
            except pickle.UnpickleableError:
                no_pickle_keys.append(key)  # This key has an unpicklable value
        return no_pickle_keys

    def invalid_obj_pickle_attrs(self, thisobj):
        no_pickle_attrs = list()
        for attr, val in vars(thisobj).items():
            try:
                pickle.dumps(getattr(thisobj, attr))
            except TypeError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
            except pickle.PicklingError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
            except pickle.UnpickleableError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
        return no_pickle_attrs


class ControllerQueue(object):
    """A set of queues to manage a continuous hot TaskMgr work loop"""

    def __init__(self):
        ## to and from are with respect to the (client) controller object
        self.from_taskmgr_q = py3_mp_queue()  # sent to the controller from TaskMgr
        self.to_taskmgr_q = py3_mp_queue()  # sent from the controller to TaskMgr
