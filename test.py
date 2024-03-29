import time

from polymer.Polymer import ControllerQueue, TaskMgr
from polymer.abc_task import BaseTask

class SimpleTask(BaseTask):
    def __init__(self, text="", wait=0.0):
        # calling super() is required...
        super(SimpleTask, self).__init__()
        self.text = text
        self.wait = wait

    def run(self):
        """run() is where all the work is done; this is called by TaskMgr()"""
        ## WARNING... using try / except in run() could squash Polymer's
        ##      internal error logging...
        print("Example BaseTask() worker")
        print("   Putting SimpleTask().run() to sleep for 2 seconds...")
        time.sleep(2.0)

    def __eq__(self, other):
        """Define how tasks are uniquely identified"""
        if isinstance(other, SimpleTask) and (other.text==self.text):
            return True
        return False

    def __repr__(self):
        return """<{0}, text: '{1}'>""".format(self.__class__.__name__, self.text)

    def __hash__(self):
        return id(self)

def Controller():
    """Controller() builds a list of tasks, and queues them to the TaskMgr
    There is nothing special about the name Controller()... it's just some
    code to build a list of SimpleTask() instances."""

    all_tasks = []

    ## Build ten tasks... do *not* depend on execution order...
    num_tasks = 401
    for ii in range(0, num_tasks):
        all_tasks.append(SimpleTask(text="SimpleTask() instance number: {0}".format(ii), wait=ii))

    targs = {
        'work_todo': all_tasks,      # a list of SimpleTask() instances
        'hot_loop': False,           # If True, continuously loop over the tasks
        'worker_count': 18,          # Number of workers (default: 5)
        'resubmit_on_error': False,  # Do not retry errored jobs...
        'queue': ControllerQueue(),
        'worker_cycle_sleep': 0.001, # Worker sleep time after a task
        'log_stdout': False,         # Don't log to stdout (default: True)
        'log_path':  "taskmgr.log",  # Log file name
        'log_level': 3,              # Logging off is 0 (debugging=3)
        'log_interval': 10,          # Statistics logging interval
    }

    ## task_mgr reads and executes the queued tasks
    task_mgr = TaskMgr(**targs)

    ## a set() of completed task objects are returned after supervise()
    results = task_mgr.supervise()
    return results

if __name__=='__main__':
    Controller()

