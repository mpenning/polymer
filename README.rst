Summary
-------

A simple framework to run tasks in parallel.

Usage
-----

.. code:: python

   import time

   from polymer.Polymer import ControllerQueue, TaskMgr
   from polymer.abc_task import BaseTask

   class SimpleTask(BaseTask):
      def __init__(self, text="", wait=0.0):
          super(SimpleTask, self).__init__()
          self.text = text
          self.wait = wait

      def run(self):
          """run() is where all the work is done; this is called by TaskMgr()"""
          ## WARNING... using try / except in run() could squash Polymer's
          ##      internal error logging...
          time.sleep(float(self.wait/10))
          print self.text

      def __eq__(self, other):
          """Define how tasks are uniquely identified"""
          if other.text==self.text:
              return True
          return False

      def __repr__(self):
          return """<{0}, wait: {1}>""".format(self.text, self.wait)

   def Controller():
      """Controller() builds a list of tasks, and queues them to the TaskMgr
      There is nothing special about the name Controller()... it's just some
      code to build a list of SimpleTask() instances."""

      tasks = list()

      ## Build ten tasks... do *not* depend on execution order...
      num_tasks = 10
      for ii in range(0, num_tasks):
          tasks.append(SimpleTask(text="Task {0}".format(ii), wait=ii))

      targs = {
           'work_todo': tasks,  # a list of SimpleTask() instances
           'hot_loop': False,   # If True, continuously loop over the tasks
           'log_level': 0,              # Logging off (debugging=3)
           'worker_count': 3,           # Number of workers (default: 5)
           'resubmit_on_error': False,  # Do not retry errored jobs...
           'queue': ControllerQueue(),
           'worker_cycle_sleep': 0.001, # Worker sleep time after a task
       }

       ## task_mgr reads and executes the queued tasks
       task_mgr = TaskMgr(**targs)

       ## a set() of completed task objects are returned after supervise()
       results = task_mgr.supervise()
       return results

   if __name__=='__main__':
       Controller()

License
-------

GPLv3
