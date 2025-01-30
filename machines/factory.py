# -*- coding: utf-8 -*-
""" factory manager"""
import os
import uuid
import logging
import time
import threading
from collections import deque

from .target import Identifier, Target
from .common import Status, InvalidTarget
from .utils import indices_as_key
from .storages import TargetStorage, FileStorage, MemoryStorage

LOGGER = logging.getLogger(__name__)

# sleep time for Factory loop
LOOP_SLEEP_TIME = 0.001


MAIN_STORAGE = "__MAIN__STORAGE__"
TEMP_STORAGE = "__TEMP__STORAGE__"


def factory(name=None, storages=None, hold=False, dry=False, **kwargs):
    """helper function to create or return an existing factory

    Parameters
    ===
        name: str
            Factory name (new or existing)
        storages: dict of (target's name, Storage)
            Specific storages for given target types
        hold: [False]/True
            Remain in factory context until all tasks are complete
        dry: [False/True]
            dry run: use dummy factory (tasks are added but never run)
    in kwargs:
        root: path
            Path to default storage directory if storing targets data
            Used as name if 'name' not provided
        handlers: dict of (target's name, FileHandler())
            File handlers for default storage
        callback: factory callback
        nosession: [False]/True
            clear queue of pending tasks on completion (callback)
        stop_on_error: [False/True]
            quit after an error

    If a factory already exists with the same name, all storage options are ignored
        and the existing factory is returned

    """
    root = kwargs.pop("root", None)
    handlers = kwargs.pop("handlers", None)

    if root and not name:
        # use root as name
        name = root

    elif not root and not name:
        # create a unique name
        name = str(uuid.uuid4())

    if name in Factory.factories:
        # existing factory
        factory = Factory.factories[name]
    else:
        # create new factory
        if not storages:
            storages = {}

        if root and not MAIN_STORAGE in storages:
            # set main storage
            storages[MAIN_STORAGE] = FileStorage(root, handlers=handlers)

        if dry:
            factory = DryFactory(name, storages, **kwargs)
        else:
            factory = Factory(name, storages, **kwargs)

    Factory.hold_current = hold
    return factory


class Factory:
    """A factory class"""

    # consts
    MAX_TASKLIST_LENGTH = 1000

    # global attributes
    factories = {}  # all factories
    current_factory = None  # current factory context
    hold_current = False  # hold current factory

    # Factory

    def __init__(
        self,
        name,
        storages=None,
        callback=None,
        nosession=False,
        auto_cleanup=True,
        stop_on_error=False,
    ):
        """Initialize new factory"""
        assert not name in Factory.factories

        LOGGER.info("Create factory: '%s'" % name)
        self._name = name
        self._thread = None
        self._callback = callback
        self._tasklist = deque([], self.MAX_TASKLIST_LENGTH)

        self.reset_queue()  # init queue
        self.nosession = nosession  # clear queue after each stop
        self.auto_cleanup = auto_cleanup
        self.stop_on_error = stop_on_error

        # storages
        if not storages:
            storages = dict()
        elif not isinstance(storages, dict) or not all(
            isinstance(storage, TargetStorage) for storage in storages.values()
        ):
            raise ValueError("storages must be a dict of TargetStorage objects.")
        else:
            # copy storage dict
            storages = storages.copy()

        # default storage
        self.main_storage = storages.get(MAIN_STORAGE, MemoryStorage())
        self.temp_storage = storages.get(TEMP_STORAGE, None)
        self.storages = storages

        # add self to factory dict
        Factory.factories[name] = self
        self.lock = threading.Lock()
        self._stop_flag = False

    def __repr__(self):
        return "Factory(%s)" % self._name

    @property
    def name(self):
        """factory's name"""
        return self._name

    @property
    def queue_size(self):
        """return queue size"""
        return self.queue.qsize()

    @property
    def tasks(self):
        """return list tasks"""
        return tuple(self._tasklist)

    def reset_queue(self):
        """clean up queue"""
        self.queue = TaskQueue()

    def get_storage(self, target):
        """return target's storage"""
        if (target.name, target.branch) in self.storages:
            return self.storages[(target.name, target.branch)]
        elif target.name in self.storages:
            # specific storage
            return self.storages[target.name]
        elif self.temp_storage and target.temporary:
            # temp storage
            return self.temp_storage
        else:
            # main storage (general case)
            return self.main_storage

    def add_task(self, task, temp=False):
        """add task to queue"""
        LOGGER.info("Adding task to queue: %s" % str(task))

        # check target
        self.check(task.output)

        # append task to working queue
        try:
            with self.lock:
                self.queue.put(task)
        except self.queue.Duplicate:
            pass
        else:
            self._tasklist.append(task)
        # start processing (if necessary)
        self.serve()

    def read(self, targets):
        """read target(s) data from storage"""
        if isinstance(targets, Target):
            storage = self.get_storage(targets)
            return storage.read(targets)
        # else
        return [self.read(target) for target in targets]

    def write(self, target, data, mode=None):
        """write target data into storage"""
        storage = self.get_storage(target)
        storage.write(target, data, mode=mode)

    def check(self, target):
        """test target"""
        if not target:
            return
        storage = self.get_storage(target)
        try:
            storage.check(target)
        except ValueError as exc:
            raise InvalidTarget(f"Invalid target: {target} ({exc})")

    def remove(self, target):
        storage = self.get_storage(target)
        storage.remove(target)

    def location(self, target):
        """return target location if any"""
        storage = self.get_storage(target)
        return storage.location(target)

    def exists(self, targets):
        """check target(s) exists"""
        if isinstance(targets, Target):
            storage = self.get_storage(targets)
            return storage.exists(targets)
        # else
        return [self.exists(target) for target in targets]

    def callback(self, summary):
        """run callback"""
        LOGGER.debug("Running callback for factory: %s" % self)
        if self._callback:
            self._callback(summary)

        if self.nosession:
            LOGGER.info("Remove %d pending tasks" % len(self.queue))
            self.reset_queue()

        # clean up storages, if any
        if self.auto_cleanup:
            for storage in self.storages.values():
                storage.cleanup(summary)

    def hold(self):
        """wait until thread terminates"""
        if not self.serving():
            return
        LOGGER.debug("Holding factory: %s", self)
        try:
            while True:
                self._thread.join(1)
                if not self.serving():
                    break
        except KeyboardInterrupt as exc:
            pending = self.queue.qsize()
            LOGGER.info("Keyboard interrupt (%d pending).", pending)
            raise (exc)

    def stop(self):
        """stop factory (after last task)"""
        LOGGER.info("Force stopping factory: %s (%d pending)", self, self.queue.qsize())
        self._stop_flag = True

    def stopping(self):
        """return True if stop flag is raised"""
        return self._stop_flag

    def serve(self):
        """start serving tasks"""
        self._stop_flag = False
        if self.serving():
            return

        LOGGER.info("Start factory: %s (%d pending)", self, self.queue.qsize())
        self._thread = self.WorkThread(self)
        self._thread.start()

    def serving(self):
        """return True if factory is currently serving tasks"""
        if not self._thread:
            return False
        else:
            return self._thread.is_alive()

    def __enter__(self):
        """set global factory"""
        self._previous_factory = Factory.current_factory
        Factory.current_factory = self
        LOGGER.debug("Enter context of factory: %s", self)
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        """restore old factory"""
        if Factory.hold_current:
            self.hold()
        self._stop_flag = False
        LOGGER.debug("Exit context of factory: %s", self)
        Factory.current_factory = self._previous_factory
        delattr(self, "_previous_factory")

    class WorkThread(threading.Thread):
        """thread class"""

        def __init__(self, factory):
            """init new thread"""
            self.factory = factory
            super().__init__(daemon=True)

        def run(self):
            """consume tasks"""
            summary = []

            while True:
                # loop while there are non-pending tasks

                pending = set()
                updated = False

                while True:
                    # process queued items

                    task = self.factory.queue.get()

                    if not task:
                        # queue empty
                        break

                    # run task
                    status = task.safe_run()

                    # stop on error
                    if self.factory.stop_on_error and status.name == "ERROR":
                        self.factory.stop()

                    # set updated to True is sucess
                    updated = updated or (status == Status.SUCCESS)

                    if status == Status.PENDING:
                        # store task for later
                        pending.add(task)
                    else:
                        summary.append(task)

                    if self.factory.stopping():
                        # stop factory
                        break

                    # end while
                    time.sleep(LOOP_SLEEP_TIME)

                with self.factory.lock:
                    # lock factory to prevent adding new data during the following

                    for task in pending:
                        # put back pending tasks in queue for next time
                        self.factory.queue.put(task)

                    if self.factory.stopping():
                        # break from loop
                        pass
                    elif updated:
                        # keep on running
                        continue

                    # else: exit loop
                    if not pending:
                        # no task pending
                        LOGGER.info("Stopping factory: %s (empty queue)" % self.factory)
                    else:
                        LOGGER.info(
                            "Stopping factory: %s (%d tasks pending)",
                            self.factory,
                            len(pending),
                        )

                    # on quiting thread, run factory callbacks
                    self.factory.callback(summary)
                    return

                    # end factory lock

                # end while 1


class DryFactory(Factory):
    """A dummy factory for dry-runs"""

    def serve(self):
        """do nothing"""
        pass

    def hold(self):
        pass


# factory helpers
def factory_exists(name):
    return name in Factory.factories


def get_factory(name):
    return Factory.factories[name]


def get_current_factory():
    """return current active factory"""
    if not Factory.current_factory:
        raise RuntimeError("No factory is current set.")
    return Factory.current_factory


def hold():
    """wait until current processes complete"""
    if not Factory.current_factory:
        raise RuntimeError("No factory is current set.")
    return Factory.current_factory.hold()


# thread-safe, sorted queue
class TaskQueue:
    """thread-safe, sorted task-queue"""

    filename = ".tasks"

    class Duplicate(Exception):
        """Raise for attempts to put a task already in queue"""

    # class Empty(Exception):
    #     """ Raise for attempts to get a task while queue is empty """

    @property
    def lock(self):
        return self._lock

    def __init__(self, lockcb=None):
        self._tasks = []
        self._key = indices_as_key
        self._lock = threading.Lock()
        self._lockcb = lockcb

    def get(self):
        """(thread-safe) pop first task in list"""

        with self._lock:
            if not self._tasks:
                return None
            if self._lockcb:
                self._lockcb(self._tasks)
            task = self._tasks.pop(0)
            return task

    def put(self, task):
        """(thread-safe) put a task in list"""
        if task in self._tasks:
            raise self.Duplicate("Task %s already in queue" % task)
        with self._lock:
            if self._lockcb:
                self._lockcb(self._tasks)
            self._tasks.append(task)
            self._tasks.sort(key=self._key)

    def empty(self):
        """(thread-safe) check if queue empty"""
        return self.qsize() == 0

    def qsize(self):
        with self._lock:
            return len(self._tasks)

    # non thread safe
    def __iter__(self):
        """yield tasks (non-popping)"""
        return iter(self._tasks)

    def __contains__(self, task):
        return task in self._tasks

    def __len__(self):
        return self.qsize()
