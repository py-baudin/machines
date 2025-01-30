""" dependency graph """

# -*- coding: utf-8 -*-
from .common import Identifier, RejectException
from .factory import get_current_factory
from .task import Task, MetaTask
from .target import ravel_identifiers

# from .machine import Machine, MetaMachine


class DependencyGraph:
    """Dependency graph of tasks and targets"""

    def __init__(self, tasks, metatasks=None):
        """init dependency graph from list of tasks"""
        self.aggregate = any(task.aggregate for task in tasks)
        self.tasks = tasks
        self.targets = {task.output: None for task in tasks if task.output}
        self.metatasks = metatasks
        for task in tasks:
            task.graph = self

    @classmethod
    def generate(
        cls,
        machines,
        indices=None,
        branches=None,
        output_indices=None,
        output_branches=None,
        parameters=None,
        **kwargs,
    ):
        if not parameters:
            parameters = {}

        # solve machine
        machines, metatasks = solve_machines(machines, parameters)

        # input identifiers
        input_ids = ravel_identifiers(indices, branches)

        # is aggregating
        aggregating = get_aggregate(machines)

        # keep temporary record of targets
        tasks = []
        for machine in machines:

            # machine dependencies abd requirements
            deps = get_dependencies(machines, machine)
            reqs = get_requirements(machines, machine)

            if aggregating:
                # update current input ids post-aggregation
                if get_aggregate(reqs) == "index":
                    input_indices = [id.index for id in input_ids]
                    current_input_ids = ravel_identifiers(input_indices, None)

                elif get_aggregate(reqs) == "branch":
                    input_branches = [id.branch for id in input_ids]
                    current_input_ids = ravel_identifiers(None, input_branches)

                elif get_aggregate(reqs):
                    current_input_ids = [Identifier(None, None)]

                else:  # pre aggregation
                    current_input_ids = input_ids

                if deps:
                    # non final machines
                    current_output_indices = None
                    current_output_branches = None
                else:
                    # final machines
                    current_output_indices = output_indices
                    current_output_branches = output_branches

            else:
                # non aggregating
                if not reqs:
                    # first machines: use output branches already
                    current_input_ids = input_ids
                    current_output_indices = output_indices
                    current_output_branches = output_branches
                else:
                    # non first machines
                    if output_indices:
                        current_input_indices = output_indices
                    else:
                        current_input_indices = [id.index for id in input_ids]
                    if output_branches:
                        current_input_branches = output_branches
                    else:
                        current_input_branches = [id.branch for id in input_ids]
                    current_input_ids = ravel_identifiers(
                        current_input_indices, current_input_branches
                    )
                    current_output_indices = None
                    current_output_branches = None

            # make tasks
            _tasks = machine.apply(
                current_input_ids,
                output_indices=current_output_indices,
                output_branches=current_output_branches,
                parameters=parameters,
                **kwargs,
            )

            # store tasks
            tasks.extend(_tasks)

        # make graph
        return cls(tasks, metatasks)

    @classmethod
    def recall(cls, machines, history, meta=None):
        if not isinstance(machines, dict):
            machines = {machine.name: machine for machine in machines}

        tasks = []
        for item in history:
            machine = machines[item["name"]]
            if not "inputs" in item:
                # metamachine
                _machines, _ = machine.recall(item)
                machines.update({machine.name: machine for machine in _machines})
            else:
                _tasks = machine.recall(item, meta=meta)
                tasks.append(_tasks)

        # make graph
        return cls(tasks)

    def __len__(self):
        """number of tasks in graph"""
        return len(self.tasks)

    def run(self, mode=None, callback=None, dry=False, fallback=True):
        """run tasks in graph"""

        def graph_callback(task, msg):
            """callback wrapper to record task"""
            if task.status.name == "RUNNING" and task.output:
                self.targets[task.output] = task

        #
        # run some/all tasks
        output_targets = set(self.output_targets())

        # factory
        factory = get_current_factory()

        # check target locks
        overwrite = mode in ["overwrite", "upgrade"]
        if overwrite and any(
            factory.get_storage(target).locked(target) for target in output_targets
        ):
            raise RejectException("Some output targets are locked")

        # if overwrite or aggregate, all tasks must be run (even existing ones)
        run_all_tasks = bool(mode) or self.aggregate or not output_targets

        if run_all_tasks:
            # run all graph tasks
            remaining_tasks = list(self.tasks)
        else:
            # run only output tasks and required tasks
            remaining_tasks = [
                task for task in self.tasks if (set(task.outputs) & output_targets)
            ]

        # remaining_tasks = list(self.tasks)
        run_tasks = []

        while remaining_tasks:
            # get first task
            task = remaining_tasks.pop(0)

            # run task
            if not dry:
                task.add_callback(graph_callback)
                task.run(mode, callback, fallback)

            # store run task
            run_tasks.append(task)

            if not run_all_tasks and not task.complete() and not task.ready():
                # add parent tasks if necessary
                parent_tasks = [
                    other
                    for other in self.tasks
                    if not other in run_tasks and other.isparent(task)
                ]
                remaining_tasks.extend(parent_tasks)

        return run_tasks

    def input_machines(self):
        """return graph output machines"""
        machines = set(task.machine for task in self.tasks)
        meta_inputs, meta_outputs = get_meta_ios(machines)
        return [
            machine
            for machine in machines
            if set(meta_inputs) & set(machine.flat_inputs)
        ]

    def output_machines(self):
        """return graph output machines"""
        machines = set(task.machine for task in self.tasks)
        meta_inputs, meta_outputs = get_meta_ios(machines)
        return [
            machine
            for machine in machines
            if set(meta_outputs) & set(machine.flat_outputs)
        ]

    def output_targets(self):
        """get graph output targets"""
        output_machines = self.output_machines()
        return set(
            target
            for task in self.tasks
            for target in task.outputs
            if task.machine in output_machines
        )

    def get_trace(self, task):
        """get task trace"""
        tasks = []
        for input in flatten_targets(task.inputs):
            prev = self.targets.get(input, None)
            if not prev:
                continue
            tasks.extend(self.get_trace(prev))
        tasks.append(task)
        return tasks

    def get_parents(self, target):
        """return parent targets"""
        task = self.targets.get(target, None)
        trace = self.get_trace(task)

        # get parent tasks
        parents = set()
        for task in trace:
            parents |= set(flatten_targets(task.inputs))
        if None in parents:
            parents.remove(None)
        return list(parents)

    def get_history(self, task):
        """get history of tasks"""
        trace = self.get_trace(task)
        if not self.metatasks:
            return [task.serialize() for task in trace]
        return [item.serialize() for item in self.metatasks + trace]

    def __repr__(self):
        """print graph (can be improved)"""
        s = "Tasks:\n"
        for task in self.tasks:
            s += "\t" + str(task) + "\n"
        return s


#
# utils


def get_meta_ios(machines):
    """return meta inputs and outputs"""

    all_inputs = {io for machine in machines for io in machine.flat_inputs}
    all_outputs = {io for machine in machines for io in machine.flat_outputs}

    meta_inputs = [input for input in all_inputs if not input in all_outputs]
    meta_outputs = [output for output in all_outputs if not output in all_inputs]

    return meta_inputs, meta_outputs


def flatten_inputs(task):
    """return flattened inputs"""
    targets = task.inputs
    if task.aggregate:
        # if aggregate: flatten inputs
        targets = [target for targetlist in targets for target in targetlist]
    return targets


def flatten_targets(targets):
    """return flattened targets"""
    if not targets:
        return []
    elif isinstance(targets[0], list):
        targets = [target for targetlist in targets for target in targetlist]
    # else: no nothing
    return targets


def get_aggregate(machines):
    """return aggregate type of machine list"""
    aggregate = False
    for machine in machines:

        if machine.aggregate == "index":
            if aggregate == "branch":
                # aggregate by identifiers
                return True
            aggregate = "index"

        elif machine.aggregate == "branch":
            if aggregate == "index":
                # aggregate by identifiers
                return True
            aggregate = "branch"

        elif machine.aggregate:
            return True

    return aggregate


def get_requirements(machines, machine, reqs=None):
    """return parent machines based on input/outputs"""
    if reqs is None:
        reqs = []
    for other in machines:
        if not other.output:
            continue
        elif other in reqs or other is machine:
            continue
        elif set(other.flat_outputs) & set(machine.flat_inputs):
            # add child machine
            reqs.append(other)
            # extend to grand-children recursively
            reqs2 = get_requirements(machines, other, reqs=reqs)
            reqs.extend([m for m in reqs2 if not m in reqs])
    return reqs


def get_dependencies(machines, machine, deps=None):
    """return child machines based on input/outputs"""
    if deps is None:
        deps = []
    if not machine.output:
        return deps

    for other in machines:
        if other in deps or other is machine:
            # avoid infinite recursion loop
            continue

        elif set(machine.flat_outputs) & set(other.flat_inputs):
            # add child machine
            deps.append(other)
            # extend to grand-children recursively
            deps2 = get_dependencies(machines, other, deps=deps)
            deps.extend([m for m in deps if not m in deps])
    return deps


def solve_machines(machines, parameters=None):
    """solve meta machines into machines"""

    if not parameters:
        parameters = {}

    metatasks = []
    solved = []
    for machine in machines:
        _machines, _metaparameters = machine.solve(parameters)
        metatask = MetaTask(machine, _metaparameters)
        solved.extend(_machines)
        metatasks.append(metatask)
    return solved, metatasks
