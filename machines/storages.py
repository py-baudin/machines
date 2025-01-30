# -*- coding: utf-8 -*-
""" storages """

import logging
import uuid
import threading
from .common import Status, TargetIsLocked, TargetAlreadyExists, TargetDoesNotExist
from .target import Target
from .filedb import FileDB

LOGGER = logging.getLogger(__name__)


def withlock(func):
    """decorator for locking"""

    def wrapper(self, *args, **kwargs):
        with self.lock:
            return func(self, *args, **kwargs)

    return wrapper


# storages


class TargetStorage:
    """Target storage class"""

    def __init__(
        self,
        memory=None,
        temporary=False,
        search=None,
        comparators=None,
        name=None,
        target_lock=None,
        **callbacks,
    ):
        """Init Storage

        Parameters
        ===
            memory: any dict-like object that implements the methods:
                * __contains__
                * __getitem__
                * __setitem__
                default: dict()
            temporary: flag used in factory for cleanup
            callbacks: on_read, on_write, on_del, callback functions
                with argument target (and second argument value for on_write)
        """
        if memory is None:
            memory = {}
        self.memory = memory
        self.uuid = uuid.uuid4()
        self.name = str(name) if name is not None else str(self.uuid)
        self.temporary = temporary
        self.lock = threading.RLock()

        self.on_read = callbacks.get("on_read", None)
        self.on_write = callbacks.get("on_write", None)
        self.on_upload = callbacks.get("on_upload", None)
        self.on_download = callbacks.get("on_download", None)
        self.on_del = callbacks.get("on_del", None)
        self.on_test = callbacks.get("on_test", None)

        # custom database search
        self._search = search

        if not comparators:
            comparators = {}
        self.comparators = comparators

        # readonly targets
        self.target_lock = target_lock if target_lock else []

    def __repr__(self):
        return f"Storage({self.memory})"

    def __str__(self):
        return f"Storage({self.name})"

    @withlock
    def exists(self, target):
        """Check whether target data exists"""
        if not isinstance(target, Target):
            raise TypeError("Invalid target object: %s" % target)

        return target in self.memory

    @withlock
    def locked(self, target):
        """return True if target exists and is locked"""
        if not self.exists(target):
            return False
        return target.name in self.target_lock

    def check(self, target):
        """check if target is valid"""
        try:
            self.memory.to_path(target)
        except AttributeError:
            pass

    @withlock
    def list(self):
        """list targets in storage"""
        return list(self.memory)

    @withlock
    def failed(self):
        """return error list targets in storage"""
        try:
            iterator = iter(self.memory)
            while True:
                next(iterator)
        except StopIteration as exc:
            failed = exc.value if exc.value else []
            return failed

    @withlock
    def location(self, target):
        """return target location"""
        if isinstance(self.memory, dict):
            return "memory"
        try:
            return self.memory.location(target)
        except AttributeError:
            return "unknown"

    @withlock
    def write(self, target, data, mode=None, **kwargs):
        """Write to target"""

        if not isinstance(target, Target):
            raise TypeError("Invalid target object: %s" % target)

        if target in self.memory:
            # target exists
            if target.name in self.target_lock:
                raise TargetIsLocked("Targets '%s' are locked" % str(target.name))

            elif mode in ["test", "upgrade"]:
                # compare with previous
                is_same = self._compare(target, data)

                # callback
                if self.on_test:
                    self.on_test(target, is_same)
                LOGGER.info("Target %s comparison was: %s", target, is_same)

                if is_same or mode == "test":
                    # skip
                    return
                # else 'upgrade': overwrite existing

            elif mode == "overwrite":
                pass

            elif mode in [None, "readonly"]:
                # check target exists
                raise TargetAlreadyExists("Target %s already exists" % str(target))

            else:
                raise ValueError(f"Invalid mode value: {mode}")

        # write target (overwrite if necessary)
        LOGGER.info("writing target %s", target)
        self.memory[target] = data

        # callback
        if self.on_write:
            self.on_write(target, data, **kwargs)

    @withlock
    def read(self, target, **kwargs):
        """Read from target"""
        if not isinstance(target, Target):
            raise TypeError("Invalid target object: %s" % target)

        # callback
        if self.on_read:
            self.on_read(target, **kwargs)

        # read data
        try:
            # single target
            LOGGER.info("reading target %s", target)
            return self.memory[target]

        except KeyError:
            raise TargetDoesNotExist("Target %s does not exist" % str(target))

    @withlock
    def copy(self, source, dest):
        """duplicate source target"""
        if not self.exists(source):
            raise TargetDoesNotExist("Target %s does not exist" % source)
        if self.exists(dest):
            raise TargetAlreadyExists("Target %s already exists" % dest)
        self.write(dest, self.read(source))

    @withlock
    def remove(self, target):
        """remove target data"""
        if not isinstance(target, Target):
            raise TypeError("Invalid target object: %s" % target)
        elif target.name in self.target_lock:
            # target exists
            raise TargetIsLocked("Targets '%s' are locked" % str(target.name))
        try:
            LOGGER.info("removing target %s", target)
            del self.memory[target]
        except KeyError:
            raise TargetDoesNotExist("Target %s does not exist" % str(target))

        # callback
        if self.on_del:
            self.on_del(target)

    @withlock
    def clear(self):
        """clear all memory"""
        LOGGER.info("Clearing storage")
        targets = list(self.memory)
        for target in targets:
            self.remove(target)
        return targets

    def _compare(self, target, data):
        """compare target with previous data value"""
        # load previous
        previous = self.read(target)
        comparator = self.comparators.get(target.name)
        if comparator:
            return comparator(previous, data)
        else:
            return previous == data

    def cleanup(self, summary):
        """remove non-final targets from storage
        summary is a sequence of tasks
        (to use as factory callback)
        """
        if not self.temporary:
            # only cleanup "temporary" storages
            return

        # process only finished tasks (done, skipped or error)
        tasks = [
            task
            for task in summary
            if task.status
            in (Status.ERROR, Status.REJECTED, Status.SUCCESS, Status.SKIPPED)
        ]
        all_targets = set()
        keep_targets = set()
        for task in tasks:

            # select input targets that are stored here
            if task.aggregate:
                # from an aggregating task
                targets = set(
                    target
                    for targetlist in task.inputs
                    for target in targetlist
                    if self.exists(target)
                )
            else:
                # from a normal task
                targets = set(
                    target for target in task.inputs if target and self.exists(target)
                )

            if not targets:
                continue

            all_targets |= targets
            if task.status == Status.ERROR:
                # keep inputs targets if task had an error
                keep_targets |= targets

        # remove input targets whose task did not have an error
        remove_targets = all_targets - keep_targets
        for target in remove_targets:
            self.remove(target)

        nremove = len(remove_targets)
        nkeep = len(keep_targets)
        LOGGER.info(f"Storage {self} cleaned-up. Removed: {nremove}, kept: {nkeep}")


def MemoryStorage(**kwargs):
    """helper function to create a memory storage"""
    memory = {}
    return TargetStorage(memory, name="memory", **kwargs)


def FileStorage(
    path, converter=None, default_handler=None, handlers=None, signature=None, **kwargs
):
    """helper function to create a Target storage with FileDB"""

    # make filedb
    memory = FileDB(
        path,
        converter=converter,
        handlers=handlers,
        default_handler=default_handler,
        signature=signature,
    )
    # make storage
    return TargetStorage(memory, name=memory.root, **kwargs)
