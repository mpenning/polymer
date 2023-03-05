from __future__ import absolute_import

from loguru import logger

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

# cPickle is no longer recommended...
#     https://stackoverflow.com/a/23582505/667301
import pickle

# This works in Python3.x...
from queue import Empty, Full

global PACKAGE_NAME
PACKAGE_NAME = "Polymer"


""" Polymer.py - Manage parallel tasks
     Copyright (C) 2023 David Michael Pennington at Starbucks Coffee Company
     Copyright (C) 2021-2023 David Michael Pennington
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

    @logger.catch(reraise=True)
    def __init__(self, n=0):
        self.count = multiprocessing.Value("i", n)

    @logger.catch(reraise=True)
    def increment(self, n=1):
        """ Increment the counter by n (default = 1) """
        with self.count.get_lock():
            self.count.value += n

    @property
    @logger.catch(reraise=True)
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
    """
    A portable implementation of multiprocessing.Queue.

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

    @logger.catch(reraise=True)
    def __init__(self, *args, **kwargs):
        # Use ctx argument if using Python3.4+
        super(py3_mp_queue, self).__init__(
            *args, ctx=multiprocessing.get_context("spawn"), **kwargs
        )

        if not sys.platform == "darwin":
            self.size = SharedCounter(0)

    @logger.catch(reraise=True)
    def put(self, *args, **kwargs):
        # Infinite recursion possible if we don't use super() here
        finished = False
        logged_full = False
        while not finished:
            try:
                super(py3_mp_queue, self).put(*args, **kwargs)
                finished = True
            except Full:
                if logged_full is False:
                    logger.warning("Queue Full.  Resubmitting")
                    logged_full = True
                pass

        if not sys.platform == "darwin":
            self.size.increment(1)
        return True

    @logger.catch(reraise=True)
    def get(self, *args, **kwargs):
        try:
            item = super(py3_mp_queue, self).get(*args, **kwargs)
            if not sys.platform == "darwin":
                self.size.increment(-1)
            return item

        except Empty:
            return {}

    @logger.catch(reraise=True)
    def qsize(self):
        """ Reliable implementation of multiprocessing.Queue.qsize() """
        if sys.platform != "darwin":
            return self.size.value
        else:
            raise NotImplementedError

    @logger.catch(reraise=True)
    def empty(self):
        """ Reliable implementation of multiprocessing.Queue.empty() """
        if sys.platform != "darwin":
            return not self.qsize()
        else:
            raise NotImplementedError

    @logger.catch(reraise=True)
    def clear(self):
        """ Remove all elements from the Queue. """
        while not self.empty():
            self.get(block=False, timeout=0.05)


class Worker(multiprocessing.Process):
    """multiprocessing worker"""

    # This is on the Worker() class
    @logger.catch(reraise=True)
    def __init__(self, w_id, todo_q, done_q, default_sleep=0.00001, log_level=0):
        super().__init__()
        assert isinstance(default_sleep, int) or isinstance(default_sleep, float)
        self.w_id = w_id
        self.cycle_sleep = default_sleep  # How long the worker should sleep
        self.task = None
        self.done_q = done_q
        self.log_level = log_level

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

    # This is on the Worker() class
    @logger.catch(reraise=True)
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
            logger.debug("done_q_send({0}) Can't pickle this dict:{1}".format(dict_hash, str(msg_dict),))
            logger.debug("    Sometimes pickling problems come from open / hung TCP sockets")

            ## Verbose list of the offending key(s) / object attrs
            ## Send all output to stderr...
            err_frag1 = ("    done_q_send({0}) Offending dict keys:".format(str(dict_hash)))
            err_frag2 = " {0}".format(no_pickle_keys)
            logger.debug(err_frag1 + err_frag2)
            for key in sorted(no_pickle_keys):
                logger.debug("      msg_dict['{0}']: '{1}'".format(key, repr(msg_dict.get(key))))
                if isinstance(msg_dict.get(key), object):
                    thisobj = msg_dict.get(key)
                    no_pickle_attrs = self.invalid_obj_pickle_attrs(thisobj)
                    err_frag1 = "      done_q_send({0}) Offending attrs:{1}".format(dict_hash, os.linesep)
                    err_frag2 = " {0}".format(no_pickle_attrs)
                    logger.debug(err_frag1 + err_frag2)
                    for attr in no_pickle_attrs:
                        logger.debug("        msg_dict['{0}'].{1}: '{2}'".format(key, attr, repr(getattr(thisobj, attr))))

            logger.debug("    done_q_send({0}) keys (no problems):".format(dict_hash))
            for key in sorted(set(msg_dict.keys()).difference(no_pickle_keys)):
                logger.debug("      msg_dict['{0}']: {1}".format(key, repr(msg_dict.get(key))))

    # This is on the Worker() class
    @logger.catch(reraise=True)
    def invalid_dict_pickle_keys(self, msg_dict) -> list:
        """
        Return a list of keys that can't be pickled.  Return [] if
        there are no pickling problems with the values associated with the
        keys.  Return the list of keys, if there are problems.
        """
        no_pickle_keys = []
        for key, val in msg_dict.items():

            try:
                # pickle.HIGHEST_PROTOCOL info:
                #     https://stackoverflow.com/a/23582505/667301
                pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
                pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL)
            except TypeError:
                no_pickle_keys.append(key)  # This key has an unpicklable value
            except pickle.PicklingError:
                no_pickle_keys.append(key)  # This key has an unpicklable value
            except pickle.UnpicklingError:
                no_pickle_keys.append(key)  # This key has an unpicklable value

        return no_pickle_keys

    # This is on the Worker() class
    @logger.catch(reraise=True)
    def invalid_obj_pickle_attrs(self, thisobj):
        no_pickle_attrs = []
        for attr, val in vars(thisobj).items():
            try:
                pickle.dumps(getattr(thisobj, attr))
            except TypeError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
            except pickle.PicklingError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
            except pickle.UnpicklingError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
        return no_pickle_attrs

    # This is on the Worker() class
    @logger.catch(reraise=True)
    def message_loop(self, todo_q, done_q):
        """Loop through messages and execute tasks"""
        t_msg = {}
        while t_msg.get("state", "") != "__DIE__":
            try:
                t_msg = todo_q.get_nowait()
                if t_msg != {} and self.log_level >= 3:
                    logger.debug("Got {0} from todo_q".format(t_msg))
                self.task = t_msg.get("task", "")  # __DIE__ has no task
                if self.task != "":

                    self.task.task_start = time.time()  # Start the timer
                    # Send ACK to the controller who requested work on this task
                    self.done_q_send(
                        {"w_id": self.w_id, "task": self.task, "state": "__ACK_TASK__"}
                    )

                    # Update the sleep time with latest recommendations
                    self.cycle_sleep = self.task.worker_loop_delay


                    # Assign the result of task.run() to task.result
                    self.task.result = self.task.run()
                    self.task.task_stop = time.time()  # Seconds since epoch

                    self.done_q_send(
                        {"w_id": self.w_id, "task": self.task, "state": "__FINISHED_TASK__"}
                    )  # Ack work finished

                    self.task = None
            except Empty:
                pass
            except Full:
                time.sleep(0.1)

            ## Disable extraneous error handling...
            except Exception as eee:
                if self.task is not None:
                    self.task.task_stop = time.time()  # Seconds since epoch
                else:
                    logger.error(str(eee))
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

    @logger.catch(reraise=True)
    def __init__(self, worker_count, log_interval=5, hot_loop=False):
        assert isinstance(hot_loop, bool)

        self.worker_count = int(worker_count)
        self.log_interval = float(log_interval)
        self.hot_loop = hot_loop

        self.all_tasks_started = time.time()
        self.stats_diff_start = time.time()
        self.exec_times = []
        self.queue_times = []

        self.all_exec_times = []
        self.all_queue_times = []

    @logger.catch(reraise=True)
    def reset_stats(self):
        self.stats_diff_start = time.time()
        self.exec_times = []
        self.queue_times = []

    @property
    @logger.catch(reraise=True)
    def time_delta(self):
        return time.time() - self.stats_diff_start

    @property
    @logger.catch(reraise=True)
    def is_log_time(self):
        """
        Return True if it's time to log
        """
        if (self.time_delta >= self.log_interval):
            return True
        return False

    @logger.catch(reraise=True)
    def log_stats_message(self, final=False):
        """
        Build log message strings and reset the stats.  Final means all tasks are done and we need a final stats report for ALL runs.
        """
        assert isinstance(final, bool)
        for ii in self.exec_times:
            self.all_exec_times.append(ii)
        for ii in self.queue_times:
            self.all_queue_times.append(ii)

        time_delta = deepcopy(self.time_delta)

        min_task_time = min(self.exec_times)
        avg_task_time = sum(self.exec_times) / len(self.exec_times)
        max_task_time = max(self.exec_times)

        min_queue_time = min(self.queue_times)
        avg_queue_time = sum(self.queue_times) / len(self.queue_times)
        max_queue_time = max(self.queue_times)

        if final is False:
            time_delta = self.time_delta
            total_tasks = len(self.exec_times)
            time_worked = sum(self.exec_times)
        else:
            time_delta = time.time() - self.all_tasks_started
            total_tasks = len(self.all_exec_times)
            time_worked = sum(self.all_exec_times)

        total_work_time = self.worker_count * time_delta
        avg_task_rate = total_tasks / time_delta
        pct_busy = time_worked / total_work_time * 100.0

        if final is False:
            polling_status = ""
        else:
            polling_status = "FINAL: "
        stats_digits = 3
        task_msg = """{6}{5}Ran {0} tasks in {4} secs, {1} tasks/s; {2} workers {3}% busy""".format(
            total_tasks, round(avg_task_rate, stats_digits), self.worker_count, round(pct_busy, 1), round(time_delta, stats_digits), polling_status, os.linesep)
        # Float format props...
        #    https://stackoverflow.com/a/33219633/667301
        task_mam = """     Task run times (seconds): {:5f}/{:5f}/{:5f} (min/avg/max)""".format(
            round(min_task_time, stats_digits), round(avg_task_time, stats_digits), round(max_task_time, stats_digits)
        )
        # Float format props...
        #    https://stackoverflow.com/a/33219633/667301
        queue_mam = """     Time in queue  (seconds): {:5f}/{:5f}/{:5f} (min/avg/max)""".format(
            round(min_queue_time, stats_digits), round(avg_queue_time, stats_digits), round(max_queue_time, stats_digits)
        )

        # Reset stats begin time...
        if final is False:
            self.reset_stats()

        return """{0}\n{1}\n{2}""".format(task_msg, task_mam, queue_mam)


class TaskMgr(object):
    """Manage tasks to and from workers; maybe one day use zmq instead of
    multiprocessing.Queue"""

    # http://www.jeffknupp.com/blog/2014/02/11/a-celerylike-python-task-queue-in-55-lines-of-code/
    @logger.catch(reraise=True)
    def __init__(
        self,
        work_todo=None,
        log_level=1,
        log_stdout=True,
        log_path="taskmgr.log",
        queue=None,
        hot_loop=False,
        worker_count=5,
        log_interval=10,
        worker_cycle_sleep=0.000001,
        resubmit_on_error=False,
    ):
        if work_todo is None:
            work_todo = []
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

        self.worker_assignments = {}  # key: w_id, value: worker Process objs
        self.results = {}
        self.configure_logging()
        self.hot_loop = hot_loop
        self.retval = set({})
        self.previously_logged_state = None

        self.validate_attribute_types()

        if hot_loop is True:
            assert isinstance(queue, ControllerQueue)
            self.controller = queue
            self.supervise()  # hot_loops automatically supervise()

    @logger.catch(reraise=True)
    def validate_attribute_types(self):

        assert isinstance(self.worker_count, int)
        assert isinstance(self.log_level, int)
        assert isinstance(self.log_interval, int)
        assert isinstance(self.worker_cycle_sleep, float)
        assert isinstance(self.work_todo, list)
        assert isinstance(self.log_path, str)
        assert isinstance(self.log_stdout, bool)
        assert isinstance(self.hot_loop, bool)
        assert isinstance(self.todo_q, mpq.Queue)
        assert isinstance(self.done_q, mpq.Queue)
        assert isinstance(self.retval, set)

        return True

    @logger.catch(reraise=True)
    def configure_logging(self):
        """
        Configure logging parameters output to sys.stdout.
        """

        logger.disable(PACKAGE_NAME)
        if self.log_level > 0:

            if self.log_path != "":
                logger.add(sink=self.log_path)

            if self.log_stdout is True:
                logger.add(sink=sys.stdout)

            if not bool(self.log_path) and (not self.log_stdout):
                self.log_level = 0
            logger.enable(PACKAGE_NAME)

    @logger.catch(reraise=True)
    def supervise(self):
        """
        If not in a hot_loop, call supervise() to start the tasks.
        """
        state = "__WAITING__"
        self.retval = set({})
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
                    logger.debug(logmsg)
                self.queue_task(task)

        finished = False
        while not finished:
            try:
                if hot_loop is True:
                    # Calculate the adaptive loop delay
                    loop_delay = self.calc_wait_time(stats.exec_times)
                    self.queue_tasks_from_controller(loop_delay=loop_delay)  # queue tasks
                    time.sleep(loop_delay)

                r_msg = self.done_q.get_nowait()  # __ACK_TASK__ or __FINISHED_TASK__
                task = r_msg.get("task")
                w_id = r_msg.get("w_id")
                state = r_msg.get("state", "__WAITING__")

                # Verify that the user called `super().__init__()` in their
                #      BaseTask() subclass __init__()
                try:
                    if (task is not None) and (task.called_super_in_task_subclass is True):
                        # In this case, the user DID call `super().__init__()`
                        #    in their BaseTask() subclass __init__()
                        pass
                except NameError:
                    # In this case, the user did NOT call `super().__init__()`
                    #    in their BaseTask() subclass __init__()
                    raise NameError("You did NOT call `super().__init__()` in your BaseTask() sub-class.")


                if state == "__ACK_TASK__":
                    if self.log_level >= 2:
                        if self.previously_logged_state != state:
                            logger.info("state: {0}".format(state))
                        self.previously_logged_state = state

                    self.worker_assignments[w_id] = task
                    self.work_todo.remove(task)
                    if self.log_level >= 3:
                        logger.debug("r_msg: {0}".format(r_msg))
                    if self.log_level >= 3:
                        logger.debug("w_id={0} received task={1}".format(w_id, task))

                    self.conditional_print_interim_stats(stats)

                    # Modified 2023-03-05...
                    state = "__WAITING__"
                    if self.log_level >= 2:
                        if self.previously_logged_state != state:
                            logger.info("state: {0}".format(state))
                        self.previously_logged_state = state
                    continue

                elif state == "":
                    num_tasks = len(self.work_todo)
                    if len(self.work_todo) > 0 or self.num_tasks_in_progress > 0:
                        state = "__WAITING__"
                    else:
                        state = "__EMPTY__"

                    if self.log_level >= 2:
                        if self.previously_logged_state != state:
                            logger.info("state: {0}".format(state))
                        self.previously_logged_state = state

                elif state == "__WAITING__":
                    if self.log_level >= 2:
                        if self.previously_logged_state != state:
                            logger.info("state: {0}".format(state))
                        self.previously_logged_state = state
                    # Wating for more task updates...
                    num_tasks = len(self.work_todo)
                    self.conditional_print_interim_stats(stats)
                    if num_tasks > 0:
                        continue

                    elif self.num_tasks_in_progress > 0:
                        self.conditional_print_interim_stats(stats)
                        continue

                    else:
                        # It's unclear why we are here...
                        raise NotImplementedError

                elif state == "__FINISHED_TASK__":
                    if self.log_level >= 2:
                        if self.previously_logged_state != state:
                            logger.info("state: {0}".format(state))
                        self.previously_logged_state = state
                    now = time.time()
                    task_exec_time = task.task_stop - task.task_start
                    task_queue_time = now - task.queue_time - task_exec_time
                    stats.exec_times.append(task_exec_time)
                    stats.queue_times.append(task_queue_time)

                    if self.log_level >= 2:
                        logger.info(
                            "TaskMgr.work_todo: {0} tasks unassigned and {1} tasks in-progress".format(
                                len(self.work_todo), self.num_tasks_in_progress
                            )
                        )
                    if self.log_level >= 3:
                        logger.debug("r_msg: {0}".format(r_msg))

                    if hot_loop is False:
                        self.retval.add(task)  # Add result to retval
                        self.worker_assignments.pop(w_id)  # Delete the key
                        finished = self.are_tasks_finished()
                    else:
                        self.controller.from_taskmgr_q.put(
                            task
                        )  # Send to the controller
                        self.worker_assignments.pop(w_id)  # Delete the key

                    # Modified 2023-03-05...
                    state = "__WAITING__"

                    self.conditional_print_interim_stats(stats)
                    continue

                elif state == "__ERROR__":
                    if self.log_level >= 1:
                        if self.previously_logged_state != state:
                            logger.info("state: {0}".format(state))
                        self.previously_logged_state = state
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


                    if hot_loop is False:
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


                elif state == "__DIE__":
                    # NOTE Doing NOTHING here... why??
                    if self.log_level >= 1:
                        if self.previously_logged_state != state:
                            logger.info("state: {0}".format(state))
                        self.previously_logged_state = state


                else:
                    if len(self.work_todo) > 0:
                        logger.error("Unhandled state: '{1}' with {0} tasks todo".format(num_tasks, state))
                        raise ValueError("Unknown state: '{0}'".format(state))
                    else:
                        logger.error("Unhandled state: '{1}' with {0} tasks todo".format(num_tasks, state))

            except Empty:
                state = "__EMPTY__"
                if self.log_level >= 1:
                    if self.previously_logged_state != state:
                        logger.info("state: {0}".format(state))
                    self.previously_logged_state = state

            except Exception as ee:
                tb_str = "".join(tb.format_exception(*(sys.exc_info())))
                logger.error("ERROR: " + str(ee) + str(tb_str))

            self.conditional_print_interim_stats(stats)

            # Adaptive loop delay unless on Mac OSX... OSX delay is constant...

            # At most, sleep 50 milliseconds...
            loop_delay = self.calc_wait_time(stats.exec_times)
            time.sleep(loop_delay)

            self.respawn_dead_workers()
            finished = self.are_tasks_finished()

        assert isinstance(self.retval, set)
        if hot_loop is False:
            self.stop_workers()
            for w_id, p in self.workers.items():
                if self.log_level >= 2:
                    logger.debug("Stopping {0} with mp.join()".format(str(p)))
                p.join()

            ## Log a final stats summary...
            if self.log_level > 0:
                logger.info(stats.log_stats_message(final=True))
            return self.retval

    @logger.catch(reraise=True)
    def conditional_print_interim_stats(self, stats):
        if self.work_todo == 0:
            # Save the log message for the FINAL log message
            return False
        elif stats.is_log_time is True:
            if self.log_level >= 0:
                logger.info(stats.log_stats_message(final=False))
            return True
        return False

    @logger.catch(reraise=True)
    def calc_wait_time(self, exec_times):
        num_samples = float(len(exec_times))

        # Work around Mac OSX problematic queue.qsize() implementation...
        if sys.platform == "darwin":
            wait_time = 0.00001  # 10us loop_delay to avoid worker / done_q race

        elif num_samples > 0.0:
            # NOTE:  OSX doesn't implement queue.qsize(), I worked around the
            # problem
            queue_size = max(self.done_q.qsize(), 1.0) + max(self.todo_q.qsize(), 1.0)
            min_task_time = min(exec_times)
            wait_time = float(min_task_time) / float(queue_size)

        else:
            wait_time = 0.00001  # 10us loop_delay to avoid worker / done_q race

        # Wait at MOST 1-millisecond...
        return min(0.001, wait_time)

    @logger.catch(reraise=True)
    def queue_tasks_from_controller(self, loop_delay=0.0):
        finished = False
        while not finished:
            try:
                ## Hot loops will queue a list of tasks...
                tasklist = self.controller.to_taskmgr_q.get_nowait()
                for task in tasklist:
                    if loop_delay > 0.0:
                        task.worker_loop_delay = loop_delay
                    self.work_todo.append(task)
                    self.queue_task(task)
            except Empty:
                # Poll until empty
                finished = True
            except Exception:
                tb.print_exc()

    @logger.catch(reraise=True)
    def queue_task(self, task):
        task.queue_time = time.time()  # Record the queueing time
        self.todo_q_send({"task": task})

    @property
    @logger.catch(reraise=True)
    def num_tasks_in_progress(self):
        """Number of assigned tasks that are still in-progress."""
        return len(self.worker_assignments.keys())

    @logger.catch(reraise=True)
    def are_tasks_finished(self):
        if (len(self.work_todo) == 0) and self.num_tasks_in_progress == 0:
            return True

        elif (self.hot_loop is False) and (len(self.retval)) == self.num_tasks:
            # We need this exit condition due to __ERROR__ race conditions...
            return True

        return False

    @logger.catch(reraise=True)
    def stop_workers(self):
        stop = {"state": "__DIE__"}
        [self.todo_q_send(stop) for x in range(0, self.worker_count)]

    @logger.catch(reraise=True)
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
                    args=(w_id, self.todo_q, self.done_q, self.worker_cycle_sleep, self.log_level),
                )
                self.workers[w_id].daemon = True
                self.workers[w_id].start()

    @logger.catch(reraise=True)
    def spawn_workers(self):
        workers = {}
        for w_id in range(0, self.worker_count):
            workers[w_id] = Process(
                target=Worker,
                name="Polymer.py Worker {0}".format(w_id),
                args=(w_id, self.todo_q, self.done_q, self.worker_cycle_sleep),
            )
            workers[w_id].daemon = True
            workers[w_id].start()
        return workers

    @logger.catch(reraise=True)
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
            logger.warning(
                "in todo_q_send({0}) Can't pickle this dict:'''{1}'''{2}".format(dict_hash, msg_dict, os.linesep)
            )

            ## Verbose list of the offending key(s) / object attrs...
            err_frag1 = "    todo_q_send({0}) Offending dict keys: {1}{2}".format(dict_hash, no_pickle_keys, os.linesep)
            err_frag2 = " {0}{1}".format(no_pickle_keys, os.linesep)
            logger.warning(err_frag1 + err_frag2)

            for key in sorted(no_pickle_keys):
                logger.debug("      msg_dict['{0}']: '{1}'".format(key, repr(msg_dict.get(key))))
                if isinstance(msg_dict.get(key), object):
                    thisobj = msg_dict.get(key)
                    no_pickle_attrs = self.invalid_obj_pickle_attrs(thisobj)
                    err_frag1 = "      todo_q_send({0}) Offending attrs:".format(dict_hash)
                    err_frag2 = " {0}".format(no_pickle_attrs)
                    logger.warning(err_frag1 + err_frag2)
                    for attr in no_pickle_attrs:
                        logger.debug("        msg_dict['{0}'].{1}: '{2}'".format(key, attr, repr(getattr(thisobj, attr))))

            logger.debug("    {0}todo_q_send({1}) keys (no problems):{2}{3}".format(dict_hash))
            for key in sorted(set(msg_dict.keys()).difference(no_pickle_keys)):
                logger.debug("      msg_dict['{0}']: {1}".format(key, repr(msg_dict.get(key))))

    @logger.catch(reraise=True)
    def invalid_dict_pickle_keys(self, msg_dict):
        """Return a list of keys that can't be pickled.  Return [] if
        there are no pickling problems with the values associated with the
        keys.  Return the list of keys, if there are problems."""
        no_pickle_keys = []
        for key, val in msg_dict.items():
            try:
                pickle.dumps(key)
                pickle.dumps(val)
            except TypeError:
                no_pickle_keys.append(key)  # This key has an unpicklable value
            except pickle.PicklingError:
                no_pickle_keys.append(key)  # This key has an unpicklable value
            except pickle.UnpicklingError:
                no_pickle_keys.append(key)  # This key has an unpicklable value
        return no_pickle_keys

    @logger.catch(reraise=True)
    def invalid_obj_pickle_attrs(self, thisobj):
        no_pickle_attrs = []
        for attr, val in vars(thisobj).items():
            try:
                pickle.dumps(getattr(thisobj, attr))
            except TypeError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
            except pickle.PicklingError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
            except pickle.UnpicklingError:
                no_pickle_attrs.append(attr)  # This attr is unpicklable
        return no_pickle_attrs


class ControllerQueue(object):
    """A set of queues to manage a continuous hot TaskMgr work loop"""

    @logger.catch(reraise=True)
    def __init__(self):
        ## to and from are with respect to the (client) controller object
        self.from_taskmgr_q = py3_mp_queue()  # sent to the controller from TaskMgr
        self.to_taskmgr_q = py3_mp_queue()  # sent from the controller to TaskMgr
