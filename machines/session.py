# -*- coding: utf-8 -*-
""" Session class """

import os
import logging
import uuid

from .common import Status
from .target import Branch
from .targetpath import TargetToPathExpr, WORKDIR_EXPR, TARGETDIR_EXPR
from .storages import MemoryStorage, FileStorage
from .factory import factory, MAIN_STORAGE, TEMP_STORAGE
from .graph import DependencyGraph
from .machine import MetaMachine, Machine, replay


def basic_session(toolbox, main, temp=None, dedicated=None, **kwargs):
    """init a session with main, temp and dedicated storages"""
    storages = {}

    # set storages for toolbox programs
    storages[MAIN_STORAGE] = main

    # if not temp storages: use memory storage
    storages[TEMP_STORAGE] = temp if temp else MemoryStorage()

    if dedicated:
        storages.update(dedicated)

    return Session(toolbox, storages, **kwargs)


class Session:
    """Run a Toolbox in a Factory with given storages"""

    def __init__(self, toolbox, storages, name=None, auto_cleanup=True):
        """start a session"""
        self.toolbox = toolbox

        # update storage with toolbox's outputs
        for program in toolbox.machines:
            for io in program.flat_inputs + program.flat_outputs:
                if not io.dest in storages:
                    storages[io.dest] = storages[MAIN_STORAGE]

        factory_name = name if name else uuid.uuid4()
        self.factory = factory(
            name=factory_name, storages=storages, auto_cleanup=auto_cleanup
        )
        # unique storages
        self.storages = set(storages.values())

    @property
    def info(self):
        """return info dictionary"""
        return {
            "toolbox": self.toolbox.name,
            "factory": str(self.factory.name),
            "storages": {key: str(val) for key, val in self.factory.storages.items()},
        }

    def stop(self, hold=False):
        """stop factory"""
        # stop factory
        self.factory.stop()
        if hold:
            self.factory.hold()
        # retrieve runnning tasks
        tasks = [task for task in self.factory.tasks if task.status.name == "RUNNING"]
        return tasks

    def clear(self):
        """clear new and pending tasks"""
        # retrieve runnning tasks
        tasks = [task for task in self.factory.tasks if task.status.name == "RUNNING"]
        self.factory.reset_queue()
        return tasks

    def close(self, hold=True):
        """close session : kill factory"""
        self.clear()
        if hold:
            # finish running task
            self.factory.hold()
        self.factory.factories.pop(self.factory.name)

    def hold(self):
        """hold current factory"""
        self.factory.hold()

    def run(
        self,
        program,
        hold=False,
        callback=None,
        history=None,
        machine=None,
        stop_on_error=False,
        show_all=False,
        **kwargs,
    ):
        """run program

        Parameters:
            cf. parameters in machine.__call__
            program: program to run (name will be search among the toolbox's programs)
            machine: if provided, the machine to run ('program' remains the displayed name)
            hold: if True, hold process until all tasks are finished or pending
            show_all: if True, return all tasks, including intermediary ("temporary") ones
            history: dict of task history

        """

        if not machine:
            machine = self.toolbox[program]

        # session callback
        callback = self._make_callback(callback=callback, history=history)

        # run factory
        self.factory.stop_on_error = stop_on_error
        with self.factory:
            # run machine
            tasks = machine(callback=callback, **kwargs)

        if hold:
            self.factory.hold()

        # return task info (filter out temporary tasks)
        return [task for task in tasks if show_all or not task.temporary]

    def autorun(self, program, *args, **kwargs):
        """autorun"""
        # get program relationships
        relationships = self.toolbox.relationships

        # get relevent machines
        def get_parents(item):
            machines = relationships.get(item, [])
            for machine in list(machines):
                for input in machine.input_names:
                    machines.extend(get_parents(input))
            return machines

        # run all programs
        autorun = MetaMachine.from_list(get_parents(program))
        return self.run(program, *args, machine=autorun, **kwargs)

    def replay(self, history, dry=False, mode=None, hold=False, **kwargs):
        """replay task"""

        # callback
        callback = self._make_callback(callback=None, history=history)

        with self.factory:
            # run machines
            tasks = replay(
                self.toolbox.machines,
                history=history,
                dry=dry,
                mode=mode,
                callback=callback,
            )

        if hold:
            self.factory.hold()

        # return task info (filter out temporary tasks)
        return [task for task in tasks if not task.temporary]

    def reset(self):
        """clear queue"""
        self.factory.reset_queue()

    def cleanup(self):
        """remove temporary targets"""
        targets = []
        for storage in self.storages:
            if storage.temporary:
                targets.extend(storage.clear())
        return targets

    def list(self):
        """list targets"""
        targets = set()
        for storage in self.storages:
            targets |= set(storage.list())
        return list(targets)

    # def list_storages(self, targets=None, storages=None):
    def summary(self, targets=None, storages=None):
        """list targets by storage"""
        summary = {}
        for storage in self.storages:
            if storages and not storage.name in storages:
                continue
            elif targets is None:
                _targets = storage.list()
            else:
                _targets = [target for target in targets if storage.exists(target)]
            summary[storage] = sorted(_targets)
        return summary

    def location(self, targets=None, storages=None):
        """get path of targets"""
        locations = {}
        for storage in self.storages:
            if storages and not storage.name in storages:
                continue
            elif targets is None:
                _targets = storage.list()
            else:
                _targets = [target for target in targets if storage.exists(target)]
            locations[storage] = [
                storage.location(target) for target in sorted(_targets)
            ]
        return locations

    def monitor(self, n=None, status=None, show_all=False):
        """return list of running or completed tasks

        Parameters
        ===
            n: int
                Return only n-last tasks (default: all tasks)
            status: tasks status (or list of)
                Filter tasks by status (default: all non-temporary/error tasks)
            show_all: [False], True
                Return all tasks (no filtering)

        """
        if status:
            if isinstance(status, str):
                status = [status]
            if not set(status) <= {s.name for s in Status}:
                raise ValueError("Invalid status: %s" % status)

        if not n:
            n = len(self.factory.tasks)

        tasks = []
        issues = ["ERROR", "REJECTED", "RUNNING"]
        for task in self.factory.tasks[::-1]:
            if n <= 0:
                break
            if status and not task.status.name in status:
                # filter out tasks based on status
                continue
            elif task.status.name not in issues and not show_all and task.temporary:
                # filter out temporary tasks
                continue
            tasks.append(task)
            # update n
            n -= 1

        return tasks

    def _make_callback(self, callback=None, history=None):
        """session callback"""
        callbacks = []
        if callable(callback):
            callbacks.append(callback)
        elif isinstance(callback, list):
            callbacks.extend(callback)
        elif callback:
            raise TypeError(f"Invalid callback type: {callback}")

        if history is not None:

            def callback_history(task, msg=None):
                """store history"""
                if task.status.name == "SUCCESS":
                    history[str(task.output)] = task.history

            callbacks.append(callback_history)
        return callbacks


# utils


def setup_storages(toolbox, workdir, tempdir=None, targetdirs=None, target_lock=None):
    """helper for creating dict of storages from toolbox

    Parameters
    ===
        toolbox: Toolbox object
        workdir: path to main storage directory
        tempdir: path to tempdir (if any)
        targetdirs: dict of {name: path} pairs for dedicated storages

    """
    # make storages
    storages = {}

    # common storage options
    storage_options = {"target_lock": target_lock}
    # main
    main_storage = setup_storage(workdir, toolbox=toolbox, **storage_options)
    storages[MAIN_STORAGE] = main_storage

    # temp storage
    if tempdir:
        # add temp dir
        temp_storage = setup_storage(tempdir, temporary=True)
        storages[TEMP_STORAGE] = temp_storage

    # target dirs
    if not targetdirs:
        targetdirs = {}

    # other user-io directories
    for item in targetdirs:
        target = item.get("name")
        storages[target] = setup_storage(item, toolbox=toolbox, **storage_options)

    return storages


def setup_storage(dest, toolbox=None, **storage_options):
    """setup storage based on destination dictionary and storage options"""

    if toolbox:
        storage_options = {
            "handlers": toolbox.handlers,
            "default_handler": toolbox.default_handler,
            "comparators": toolbox.comparators,
            "signature": toolbox.signature,
            **storage_options,
        }

    if isinstance(dest, dict):
        if dest.get("name"):
            # target dir
            config = {**TARGETDIR_EXPR, **dest}
        else:
            # workdir
            config = {**WORKDIR_EXPR, **dest}
        dest = config.pop("path")
        conv = TargetToPathExpr(**config)
    else:
        conv = None
    return FileStorage(dest, converter=conv, **storage_options)
