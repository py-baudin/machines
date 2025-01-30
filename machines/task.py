# -*- coding: utf-8 -*-
""" machine class """

import logging
import uuid
import inspect
import traceback
import threading
import contextvars

from .common import Identifier, Status, RejectException, ExpectedError
from .utils import task_repr
from .target import Index, Branch, Identifier, Target
from .storages import TargetDoesNotExist, TargetAlreadyExists
from .parameters import solve_parameters, ParameterError
from .factory import get_current_factory

LOGGER = logging.getLogger(__name__)

# global context variable
current_task = contextvars.ContextVar("task")


def get_context():
    """get context object"""
    try:
        task = current_task.get()
    except LookupError:
        raise RuntimeError("Cannot access task context outside of task")
    return TaskContext(task)


class Task:
    """A Machine's task"""

    def __init__(
        self, machine, inputs, output, parameters=None, meta=None, attach=None, **extra
    ):
        """Initialize a Task

        Parameters:
        ===
            machine: parent machine
            inputs: Input identifiers
            output: Output identifier
            parameters: machine parameter values
            meta: dict of task meta data


        A Task must be created within a factory context
        """

        # threading lock
        self.lock = threading.RLock()

        self.factory = get_current_factory()
        self.machine = machine

        # set inputs/output
        if not isinstance(inputs, list):
            inputs = [inputs]
        self.input_ids = [
            Identifier(Index(index), Branch(branch)) for index, branch in inputs
        ]
        self.output_id = Identifier(None, None) if not output else Identifier(*output)

        # meta
        self.meta = {} if not meta else meta

        # set unique signature
        self.uuid = uuid.uuid4()

        # info
        self.status = Status.NEW
        self.message = None
        self.error = None

        # other
        self.graph = None
        self.mode = None
        self._fallback = True
        self._callbacks = []

        # input targets
        self.available_inputs = {}
        if self.aggregate:
            # default input destinations
            self.default_inputs = [
                input.targets(self.input_ids) for input in machine.main_inputs
            ]
        else:
            # default input destinations
            self.default_inputs = [
                input.target(*self.input_ids[0]) for input in machine.main_inputs
            ]

        # output target
        if machine.output:
            self.output = machine.main_output.target(
                *self.output_id, task=self, attach=attach
            )
        else:
            self.output = None

        # parameters
        self.parameters = self._solve_parameters(parameters)
        # extra parameters (non-parsed)
        self.extra_parameters = extra

    def _solve_parameters(self, parameters):
        """parse passed parameters"""
        try:
            return solve_parameters(self.machine.all_parameters, parameters)
        except ParameterError as exc:
            raise ParameterError(f"{self}: {exc}")

    def run(self, mode=None, callback=None, fallback=True):
        """run task in factory

        Parameters:
        ===
            mode: write mode for output target (default: read-only)
            callback: task callback function
                callback signature: func(task)
        """
        self.mode = mode
        self._fallback = fallback
        self.add_callback(callback)
        self.factory.add_task(self)

    def add_callback(self, callback):
        """add and verify callback format"""
        if not callback:
            return
        elif callable(callback):
            callbacks = [callback]
        else:
            callbacks = callback

        for callback in callbacks:
            # get func signature
            fparams = inspect.signature(callback).parameters
            if len(fparams) < 2:
                raise ValueError(f"Invalid callback signature: {callback}")
            self._callbacks.append(callback)

    @property
    def aggregate(self):
        return self.machine.aggregate

    @property
    def requires(self):
        return self.machine.requires

    @property
    def fallback(self):
        """forbid branch fallback if requires == 'any'"""
        return self._fallback and (self.requires == "all")

    @property
    def name(self):
        """task's machine name"""
        return self.machine.name

    @property
    def identifier(self):
        """return task's identifier"""
        if self.machine.output:
            return self.output_id
        return Identifier(None, None)

    @property
    def index(self):
        if self.machine.output:
            return self.output.index
        return Index(None)

    @property
    def branch(self):
        if self.machine.output:
            return self.output.branch
        return Branch(None)

    @property
    def output_data(self):
        output = self.output
        if output:
            return self.factory.read(output)

    @property
    def inputs(self):
        """return actual inputs"""
        return [
            self.available_inputs.get(name) or default
            for name, default in zip(self.machine.input_names, self.default_inputs)
        ]

    @property
    def flat_inputs(self):
        """return flattened inputs"""
        inputs = self.inputs
        if not self.aggregate:
            return inputs
        return [input for inputlist in inputs for input in inputlist]

    @property
    def outputs(self):
        if not self.machine.output:
            return []
        return [self.output]

    @property
    def targets(self):
        """inputs and output targets"""
        targets = dict(self.available_inputs)
        if self.output:
            targets[self.machine.output_name] = self.output
        return targets

    @property
    def storage(self):
        """return storage of output target"""
        if self.output:
            return self.factory.get_storage(self.output)

    @property
    def temporary(self):
        """return True of task.output is stored in temporary storage"""
        return bool(self.output) and (self.output.temp)
        # return bool(self.storage) and self.storage.temporary

    @property
    def trace(self):
        """get task's trace"""
        if not self.graph:
            raise RuntimeError("Task's graph is not set")
        return self.graph.get_trace(self)

    @property
    def history(self):
        """get task's history"""
        if not self.graph:
            raise RuntimeError("Task's graph is not set")
        return self.graph.get_history(self)

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        return self.uuid == other.uuid

    def __repr__(self):
        return task_repr(self)

    def serialize(self):
        """serialize task"""
        return {
            "name": self.machine.name,
            "inputs": self.input_ids,
            "output": None if not self.output_id else self.output_id,
            "parameters": self.parameters,
            "extra": self.extra_parameters,
        }

    @classmethod
    def deserialize(cls, machine, info, meta=None):
        """create task from serialized info"""
        return cls(
            machine,
            info["inputs"],
            info["output"],
            parameters=info["parameters"],
            extra=info.get("extra"),
            meta=meta,
        )

    def ischild(self, other):
        """check if task's output can be used as input to self"""
        if not other.output:
            # has no output
            return False
        elif other.output.name not in [io.dest for io in self.machine.flat_inputs]:
            # output name does not match
            return False
        elif not other.output.identifier in self.input_ids:
            # indices do not match
            return False
        return True

    def isparent(self, other):
        """check if self.output can be used as task's input"""
        return other.ischild(self)

    def _update(self):
        """update available inputs according to aggregate parameter

        Note: not-thread safe. TODO: use thread-locks
        """
        if not self.status in [Status.NEW, Status.PENDING]:
            # do not update if task is running or finished
            return

        # found inputs
        input_names = self.machine.input_names
        found_inputs = {name: [] for name in self.machine.inputs}

        # find available inputs
        for id in self.input_ids:

            # targets for each input
            targets = {}

            for name, inputs in self.machine.inputs.items():
                index, branch = id

                while True:
                    # branch fallback loop

                    for input in inputs:
                        # loop over alternative inputs
                        target = input.target(index, branch)

                        if self.factory.exists(target):
                            LOGGER.info(f"{self}: found target {target}")
                            targets[name] = target
                            break
                    else:
                        # not found: Branch fallback
                        if self.fallback and branch != None:
                            branch = branch.crop(1)
                            continue
                        # else: no target found
                        LOGGER.info(
                            f"{self}: no target found for input: '{input.dest}'"
                        )

                    # end fallback loop
                    break

            # skip targets if no target has the correct branch
            if all(target.branch != id.branch for target in targets.values()):
                continue

            for name in targets:
                found_inputs[name].append(targets[name])

        if not self.aggregate:
            # not aggregating: use first (only) target found
            self.available_inputs = {
                name: next(iter(found_inputs[name]), None) for name in input_names
            }
        else:  # aggregating
            self.available_inputs = found_inputs

    def ready(self):
        """return True if task can be run

        Note: not thread-safe (see self.update)
        """
        if not self.machine.inputs:
            return True

        # else check inputs
        with self.lock:
            self._update()
        _join = {"all": all, "any": any}[self.requires]
        return bool(self.available_inputs) & bool(_join(self.available_inputs.values()))

    def complete(self):
        """return True if output target exists"""
        if not self.output:
            # if no output, there is no target
            return False
        return self.factory.exists(self.output)

    def callback(self, msg=None):
        """(called by factory)"""
        for callback in self._callbacks:
            callback(self, msg=msg)

    def attach(self, **kwargs):
        """shortcut to attach info to output target"""
        if self.output:
            self.output.attach(**kwargs)

    def _load_input_data(self):
        """load input data"""
        input_data = {}
        input_ids = {}
        input_attachments = {}
        for name, target in self.available_inputs.items():
            if not target:
                # skip if input does not exist
                continue

            if isinstance(target, Target):
                # single target
                input_data[name] = self.factory.read(target)
                input_ids[name] = target.identifier
                input_attachments[name] = target.attachment

            elif isinstance(target, list):
                # several targets (aggregate)
                indices = []
                attachments = []
                data = []
                locations = []
                for _target in target:
                    try:
                        data.append(self.factory.read(_target))
                        indices.append(_target.identifier)
                        attachments.append(_target.attachment)
                    except RejectException:
                        pass  # ignore exception at this point
                if not data:
                    # raise exception
                    raise RejectException(f"All input data for {name} were rejected")
                input_data[name] = data
                input_ids[name] = indices
                input_attachments[name] = attachments

        # return loaded data
        return input_data, input_ids, input_attachments

    def _make_args(self):
        """prepare func arguments"""
        machine = self.machine
        func = machine.func
        inputs = self.available_inputs
        input_groups = machine.input_groups

        # pass targets
        targets = dict(inputs)

        # get func signature
        fparams = inspect.signature(func).parameters

        # load input data
        data, indices, attachments = self._load_input_data()
        if self.output:
            indices[machine.output_name] = self.output.identifier
            attachments[machine.output_name] = self.output.attachment
            targets[machine.output_name] = self.output

        args = {}
        for name in fparams:
            if "inputs" == name:
                # special case: all inputs
                args[name] = data
            elif "identifiers" == name:
                # special case: all indices
                args[name] = indices
            if "identifier_output" == name:
                # special case: identifier output
                args[name] = None if not self.output else self.output.identifier
            elif "targets" == name:
                # special case: all indices
                args[name] = targets
            elif "attachments" == name:
                # special case: all attachments
                args[name] = attachments
            elif "meta" == name:
                # attach task meta data
                args[name] = self.meta
            elif name in input_groups:
                # grouped inputs
                args[name] = {input: data[input] for input in input_groups[name]}
            elif name in inputs:
                # input data
                default = None if not self.aggregate else []
                args[name] = data.get(name, default)
            elif name.startswith("identifier_"):
                # identifier
                default = None if not self.aggregate else []
                args[name] = indices.get(name[len("identifier_") :], default)
            elif name.startswith("attachment_"):
                # attachments
                default = None if not self.aggregate else []
                args[name] = attachments.get(name[len("attachment_") :], default)
            elif name in self.parameters:
                # parameters
                args[name] = self.parameters[name]
            elif name in self.extra_parameters:
                args[name] = self.extra_parameters[name]
        return args

    def safe_run(self):
        """run a task (called by factory)"""
        if not self.status in [Status.NEW, Status.PENDING]:
            raise RuntimeError(f"Task {self} has terminated, cannot run again")

        def update_status(status, msg=None):
            """set status, run callback, returns status"""
            self.status = status
            try:
                self.callback(msg)
            except Exception as exc:
                LOGGER.info("Task %s: an error occured during callback: %s", self, exc)
            return self.status

        with self.lock:
            if self.complete() and not self.mode:
                # check if task needed
                LOGGER.info("Target %s already exists, skipping", str(self))
                return update_status(Status.SKIPPED)

            elif not self.ready():
                # check if task ready
                LOGGER.info("Task %s not ready, pending" % str(self))
                return update_status(Status.PENDING)

            LOGGER.info("Task %s: running" % str(self))
            update_status(Status.RUNNING)

        # setup context
        current_task.set(self)
        ctx = contextvars.copy_context()

        # run task
        try:
            args = self._make_args()
            # return_value = self.machine.func(**args)
            return_value = ctx.run(self.machine.func, **args)

        except RejectException as exc:
            msg = str(exc)
            self.message = msg
            LOGGER.info("Task %s was rejected (%s)", self, msg)
            return update_status(Status.REJECTED, msg)

        except ExpectedError as exc:
            msg = str(exc)
            self.message = msg
            LOGGER.info("Task %s had an expected error on running (%s)", self, msg)
            return update_status(Status.ERROR, msg)

        except Exception as exc:
            # error at running
            tb = traceback.format_exc()
            LOGGER.info("Task %s: an error occured while running", self)
            LOGGER.info(tb)
            self.error = (str(exc), str(tb))
            return update_status(Status.ERROR, exc)

        if self.output:
            # store output
            try:
                self.factory.write(self.output, return_value, mode=self.mode)
            except Exception as exc:
                # error at writing
                tb = traceback.format_exc()
                LOGGER.info("Task %s: an error occured while writing output" % self)
                LOGGER.info(tb)
                self.error = (str(exc), str(tb))
                return update_status(Status.ERROR, exc)

        # success
        LOGGER.info("Task %s: done" % str(self))
        return update_status(Status.SUCCESS)


class MetaTask:
    """metamachine task"""

    def __init__(self, metamachine, parameters):
        self.metamachine = metamachine
        self.parameters = parameters

    def serialize(self):
        """serialize meta-task"""
        return {"name": self.metamachine.name, "parameters": self.parameters}

    @classmethod
    def deserialize(cls, metamachine, history):
        """create task from serialized info"""
        parameters = history["parameters"]
        return metamachine.solve(parameters)


class TaskContext:
    """namespace for task context info"""

    def __init__(self, task):
        self.meta = task.meta
        self.inputs = task.machine.input_names
        self.output = task.machine.output_name
        self.targets = {}
        self.indices = {}
        self.identifiers = {}
        self.branches = {}
        self.attachments = {}

        for name, target in task.targets.items():
            if not target:
                continue
            elif isinstance(target, Target):
                self.targets[name] = target
                self.indices[name] = target.index
                self.branches[name] = target.branch
                self.identifiers[name] = target.identifier
                self.attachments[name] = target.attachment
            else:
                self.targets[name] = list(target)
                self.indices[name] = [tgt.index for tgt in target]
                self.branches[name] = [tgt.branch for tgt in target]
                self.identifiers[name] = [tgt.identifier for tgt in target]
                self.attachments[name] = [tgt.attachment for tgt in target]
