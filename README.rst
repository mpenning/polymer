Summary
-------

A simple framework to run tasks in parallel.

Usage
-----

.. code:: python

   from multiprocessing import Process, Queue
   from Queue import Empty, Full
   import time

   from polymer.Polymer import ControllerQueue, TaskMgr
   from polymer.abc_task import BaseTask

   class SimpleTask(BaseTask):
      def __init__(self, text="", wait=0.0):
          super(SimpleTask, self).__init__()
          self.text = text
          self.wait = wait

      def run(self):
          time.sleep(float(self.wait/10))
          print self.text

      def __eq__(self, other):
          """Define how tasks are uniquely identified"""
          if other.text==self.text:
              return True
          return False

      def __repr__(self):
          return """<{0}, wait: {1}>""".format(self.text, self.wait)

   class Controller(object):
      """Controller() builds a list of tasks, and queues them to the TaskMgr"""
      def __init__(self):
          c_q = ControllerQueue()

          tasks = list()
          ## Build ten tasks... do *not* depend on execution order...
          num_tasks = 10
          for ii in range(1, num_tasks):
              tasks.append(SimpleTask(text="Task {0}".format(ii), wait=ii))

          args = {
              'queue': c_q,
              'work_todo': tasks,
              'log_level': 0,
              'worker_count': 3,
              'worker_cycle_sleep': 0.001,
          }

          ## task_mgr reads and executes the queued tasks
          task_mgr = TaskMgr(**args)

          ## a set() of completed task objects are returned after supervise()
          results = task_mgr.supervise()

   if __name__=='__main__':
      Controller()

License
-------

GPLv3
