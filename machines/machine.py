# -*- coding: utf-8 -*-
""" machine class """

import logging
import inspect

from .common import Identifier
from .utils import obj_repr, indices_as_key
from .target import Branch, Index, Identifier, Target
from .io import Input, Output, parse_io
from .parameters import Parameter, VariableIO, Freeze, setup_parameter, solve_parameters
from .task import Task, MetaTask
from .graph import DependencyGraph, get_meta_ios

LOGGER = logging.getLogger(__name__)
AGGREGATE_CHOICE = {False, True, "ids", "index", "branch"}
REQUIRES_CHOICE = {"any", "all"}


class Machine:
    """process creation class"""

    # allow multiple outputs
    multi_outputs = False

    def __init__(
        self,
        func,
        input=None,
        inputs=None,
        output=None,
        outputs=None,
        requires="all",
        aggregate=False,
        description=None,
        groups=None,
        parameters={},
        **kwargs,
    ):
        """initialize Machine object

        Parameters
        ===
            func: function
                processing function
            inputs: str, TargetType (or list/dict of)
                input target types
            output: None, str, TargetType
                output target type (if any)
            requires: {"all", "any"}
                * "all": all input targets with matching id/branch must exist
                * "any": at least one input target with matching id/branch must exist
            aggregate: None/str among {False, True, "ids", "index", "branch"}
                Specify whether whether and how func is aggregating targets
                * False: no aggregation
                * "ids": aggregate all parent tasks
                * "index": aggregate all indices (keep separate branches)
                * "branch": aggregate all branches (keep separate indices)
            parameters: dict of Parameter initializers/objects
            groups: dict of input name groups

        """
        # store function
        self.func = func
        self._func_signature = list(inspect.signature(func).parameters)

        if not aggregate in AGGREGATE_CHOICE:
            raise ValueError(f"'aggregate' must be chosen among: {AGGREGATE_CHOICE}")
        if not requires in REQUIRES_CHOICE:
            raise ValueError(f"'requires' must be chosen among: {REQUIRES_CHOICE}")
        self.aggregate = aggregate
        self.requires = requires

        # i/o
        self.inputs = {}
        self.outputs = {}
        self.input_groups = groups if groups else {}

        # parameters
        self.parameters = {}
        self.frozen_parameters = {}

        # name and description
        self.name = func.__name__
        if not description and func.__doc__:
            description = func.__doc__.strip()  # get docstring
        self.description = description

        # parse inputs / outputs
        inputs = inputs if inputs else input
        for name, alts in parse_io(inputs).items():
            self.set_input(name, alts)

        outputs = outputs if outputs else output
        for name, alts in parse_io(outputs).items():
            self.set_output(name, alts)

        # parse parameters
        parameters = {**kwargs, **parameters}
        for name, parameter in parameters.items():
            self.set_parameter(name, setup_parameter(parameter))

    def set_input(self, name, inputs, group=None, replace=False):
        """add/replace input"""
        if replace:
            self.inputs.pop(name, None)

        inputs = [inputs] if not isinstance(inputs, list) else inputs
        for input in inputs:
            if not isinstance(input, Input):
                raise TypeError(f"Invalid input type: {input}")
            self.inputs.setdefault(name, []).append(input)

        if group:
            self.input_groups.setdefault(group, []).append(name)
        self._check_signature()

    def set_output(self, name, outputs, replace=False):
        """add new output"""
        outputs = [outputs] if not isinstance(outputs, (list, tuple)) else outputs
        if replace:
            self.outputs.pop(name, None)

        elif not self.multi_outputs and (len(self.outputs) or len(outputs) > 1):
            raise ValueError(f"Multiple outputs are not authorized")

        elif name in self.outputs:
            raise ValueError(
                f"Output {name} already set, alternative outputs are not authorized"
            )

        for output in outputs:
            if not isinstance(output, Output):
                raise TypeError(f"Invalid output type: {output}")
            self.outputs.setdefault(name, []).append(output)

        self._check_signature()

    def set_parameter(self, name, parameter, replace=False):
        """add parameter to the machine"""
        if replace:
            self.parameters.pop(name, None)

        if not isinstance(parameter, Parameter):
            raise TypeError(f"Invalid parameter type: {parameter}")

        elif name in self.all_parameters:
            raise ValueError(f"Parameter: {name} already set")

        elif isinstance(parameter.type, Freeze):
            # store value in frozen parameters
            self.frozen_parameters[name] = parameter
        else:
            # set parameter
            self.parameters[name] = parameter

        # check
        self._check_signature()

    def _check_signature(self):
        """check machine signature"""
        inputs = set(self.inputs)
        outputs = set(self.outputs)
        variable_ios = set(self.variable_ios)
        parameters = set(self.all_parameters) - variable_ios
        groups = self.input_groups

        if parameters & ((outputs | inputs) - variable_ios):
            overlap = parameters & ((outputs | inputs) - variable_ios)
            raise ValueError(f"Overlapping parameters/ios: {overlap}")

        # function signature
        signature = set(self._func_signature)
        gp_signature = {input for inputs in groups.values() for input in inputs}
        if "inputs" in signature:
            pass
        elif not set(groups) <= signature:
            missing = list(set(groups) - signature)
            raise ValueError(f"Missing group(s) in function definition: {missing} ")
        elif not inputs <= signature | gp_signature:
            missing = list(inputs - signature | gp_signature)
            raise ValueError(f"Missing input(s) in function definition: {missing} ")

        # if not parameters <= signature:
        if not parameters - variable_ios <= signature:
            missing = list(parameters - signature)
            raise ValueError(f"Missing parameters(s) in function definition: {missing}")

    @property
    def info(self):
        """return Machine info"""
        return {
            "name": self.name,
            "inputs": {
                name: [{"dest": io.dest, "type": io.type} for io in ios]
                for name, ios in self.inputs.items()
            },
            "outputs": {
                name: [{"dest": io.dest, "type": io.type} for io in ios]
                for name, ios in self.outputs.items()
            },
            "variable": {
                name: (
                    None
                    if vio.required
                    else {
                        "default": getattr(vio.default, "dest", None),
                        "default-type": getattr(vio.default, "type", None),
                    }
                )
                for name, vio in self.variable_ios.items()
            },
            "groups": self.input_groups,
            "parameters": {
                name: self.parameters[name].info for name in self.parameters
            },
            "aggregate": self.aggregate,
            "description": self.description,
        }

    @property
    def input_names(self):
        return [name for name in self.inputs]

    @property
    def output_names(self):
        return [name for name in self.outputs]

    @property
    def output_name(self):
        """single output"""
        return None if not self.outputs else next(iter(self.outputs))

    @property
    def output(self):
        """return single output"""
        name = self.output_name
        return None if not name else self.outputs[name]

    @property
    def flat_inputs(self):
        return [io for ios in self.inputs.values() for io in ios]

    @property
    def flat_outputs(self):
        return [io for ios in self.outputs.values() for io in ios]

    @property
    def main_inputs(self):
        return [ios[0] for ios in self.inputs.values()]

    @property
    def main_outputs(self):
        return [ios[0] for ios in self.outputs.values()]

    @property
    def main_output(self):
        name = self.output_name
        return None if name is None else self.outputs[name][0]

    @property
    def all_parameters(self):
        return {**self.parameters, **self.frozen_parameters}

    @property
    def variable_ios(self):
        return {
            name: param
            for name, param in self.parameters.items()
            if isinstance(param.type, VariableIO)
        }

    def __repr__(self):
        return obj_repr(self.name, **self.parameters)

    # helper functions
    def __call__(self, *args, **kwargs):
        """create and run all tasks corresponding to the passed ids and branches
        Parameters
        ===
            indices: Index (or list of)
                Input index(ices)
            branches: Branch (or list of)
                Input branch(es)
            output_ids: Index (or list of)
                Output id(s) (defaults to input ids).
            output_branches: Branch (or list of)
                Output branch(es) (default to input branch).
            Other kwargs: parameters, callback, mode, meta: cf. Task
        Return
        ===
            list of created tasks
        """
        if "identifiers" in kwargs:
            identifiers = [Identifier(*id) for id in kwargs.pop("identifiers")]
            kwargs["indices"] = [id.index for id in identifiers]
            kwargs["branches"] = [id.branch for id in identifiers]

        if not kwargs.get("parameters"):
            kwargs["parameters"] = {}

        # retrieve parameters from kwargs
        kwargs["parameters"].update(
            {name: kwargs.pop(name) for name in self.parameters if name in kwargs}
        )

        return autorun([self], *args, **kwargs)

    def single(self, *args, **kwargs):
        """Generate a single task"""
        tasks = self.__call__(*args, **kwargs)
        if len(tasks) > 1:
            LOGGER.warning("Multiple tasks were created, returning first one only")
        return tasks[0]

    def replay(self, history, **kwargs):
        """replay serialized task"""
        return replay([self], history, **kwargs)

    def solve(self, parameters):
        """return list of machines"""
        return self._solve_variable_ios(parameters)

    def _solve_variable_ios(self, parameters):
        """create new machine with parameter-set inputs/outputs"""
        if not self.variable_ios:
            return [self], {}

        # get variable i/o parameters
        param_values = solve_parameters(self.variable_ios, parameters)

        # variable inputs
        inputs = {}
        for name, alts in self.inputs.items():
            if name in param_values and param_values[name] is None:
                # remove inputs set to None
                continue
            elif name in param_values:
                # replace input with variable input
                inputs[name] = [param_values[name]]
            else:
                inputs[name] = alts

        # variable outputs
        outputs = {}
        for name, alts in self.outputs.items():
            if name in param_values and param_values[name] is None:
                # remove outputs set to None
                continue
            if name in param_values:
                # replace output with variable output
                outputs[name] = [param_values[name]]
            else:
                outputs[name] = alts

        new_machine = self.copy(inputs=inputs, output=outputs)
        return [new_machine], param_values

    def recall(self, history, meta=None):
        """create task from history"""
        task = Task.deserialize(self, history, meta=meta)
        return task

    def apply(self, identifiers, **kwargs):
        """return list of tasks"""
        # map/group targets
        if not self.aggregate:
            tasks = self._map(identifiers, **kwargs)
        else:
            tasks = self._aggregate(identifiers, **kwargs)
        return sorted(tasks, key=indices_as_key)

    def _map(self, identifiers, output_indices=None, output_branches=None, **kwargs):
        """map input identifiers to output identifiers"""
        if not output_indices:
            # copy input id
            output_indices = [id.index for id in identifiers]
        elif not isinstance(output_indices, list):
            output_indices = [output_indices]

        if not output_branches:
            # copy input branch
            output_branches = [id.branch for id in identifiers]
        elif not isinstance(output_branches, list):
            # extend branch
            output_branches = [
                Branch(id.branch) + Branch(output_branches) for id in identifiers
            ]

        output_ids = [Identifier(*id) for id in zip(output_indices, output_branches)]

        if len(identifiers) != len(output_ids):
            raise ValueError("Incompatible numbers of input and output identifiers")

        # dispatch parameters for each index
        indexwise_parameters = dispatch_parameters(
            identifiers, kwargs.pop("parameters", {})
        )

        # make tasks
        tasks = []
        for input_id, output_id in zip(identifiers, output_ids):
            # make task
            parameters = indexwise_parameters[input_id]
            task = Task(self, input_id, output_id, parameters=parameters, **kwargs)

            tasks.append(task)
        return tasks

    def _aggregate(
        self, identifiers, output_indices=None, output_branches=None, **kwargs
    ):
        """group input tasks"""

        if self.aggregate in [True, "ids"]:
            id_groups = [(None, None)]
            match = lambda i1, i2: True

        elif self.aggregate == "index":
            id_groups = set((None, id.branch) for id in identifiers)
            match = lambda i1, i2: i1[1] == i2[1]
        elif self.aggregate == "branch":
            id_groups = set((id.index, None) for id in identifiers)
            match = lambda i1, i2: i1[0] == i2[0]
        else:
            raise ValueError(f"Invalid value for aggregate: {self.aggregate}")

        if not output_indices:
            # copy input id
            output_indices = [gp[0] for gp in id_groups]
        elif not isinstance(output_indices, list):
            output_indices = [output_indices]

        if not output_branches:
            # copy input branch
            output_branches = [gp[1] for gp in id_groups]
        elif not isinstance(output_branches, list):
            # extend existing branches
            output_branches = [
                Branch(gp[1]) + Branch(output_branches) for gp in id_groups
            ]

        output_ids = [Identifier(*id) for id in zip(output_indices, output_branches)]

        if len(id_groups) != len(output_ids):
            raise ValueError("Incompatible numbers of input and output identifiers")

        # make tasks
        _output = None
        tasks = []
        for group, output_id in zip(id_groups, output_ids):
            input_ids = [id for id in identifiers if match(group, id)]
            # make task
            task = Task(self, input_ids, output_id, **kwargs)
            tasks.append(task)
        return tasks

    def copy(self, **kwargs):
        """copy machine"""
        inputs = kwargs.pop("inputs", kwargs.pop("input", self.inputs))
        outputs = kwargs.pop("outputs", kwargs.pop("output", self.outputs))
        return self.__class__(
            self.func,
            inputs=inputs,
            outputs=outputs,
            aggregate=kwargs.pop("aggregate", self.aggregate),
            requires=kwargs.pop("requires", self.requires),
            description=kwargs.pop("description", self.description),
            groups=kwargs.pop("groups", self.input_groups),
            parameters={**self.all_parameters, **kwargs},
        )


class MetaMachine(Machine):
    """Machine creation object"""

    multi_outputs = True

    def _check_signature(self):
        """no checks"""
        pass

    @property
    def output(self):
        return None

    @property
    def meta_parameters(self):
        fparams = self._func_signature
        return {
            param: self.parameters[param]
            for param in fparams
            if param in self.parameters
        }

    @classmethod
    def from_list(cls, machines, **kwargs):
        """init MetaMachine from list of Machines"""

        # check types
        for machine in machines:
            if not isinstance(machine, Machine):
                raise ValueError("Invalid Machine type for machine %s" % str(machine))

        # inputs / output (including alternate inputs)
        meta_inputs, meta_outputs = get_meta_ios(machines)

        # parameters
        parameters = dict(
            param for machine in machines for param in machine.parameters.items()
        )
        parameters.update(kwargs)

        # update machine i/os
        # machines = update_machine_ios(machines, meta_inputs, meta_outputs)

        # func and names
        def func():
            return machines

        name = "Metamachine({})".format(str([machine.name for machine in machines]))
        func.__name__ = name

        # make machine
        return cls(func, inputs=meta_inputs, output=meta_outputs, **parameters)

    @classmethod
    def from_dict(cls, machines, default=None, **parameters):
        """create MetaMachine from dictionary"""
        choices = list(machines)

        # check types
        for choice in choices:
            if not isinstance(machines[choice], list):
                machines[choice] = [machines[choice]]
            for machine in machines[choice]:
                if not isinstance(machine, Machine):
                    raise ValueError(
                        "Invalid Machine type for machine %s" % str(machine)
                    )

        # inputs / output
        meta_inputs = set()
        meta_outputs = set()
        for choice in choices:

            # inputs / output
            _meta_inputs, _meta_outputs = get_meta_ios(machines[choice])

            # find inputs and outputs
            meta_inputs |= set(_meta_inputs)
            meta_outputs |= set(_meta_outputs)

        # parameters
        _parameters = dict(
            param
            for choice in choices
            for machine in machines[choice]
            for param in machine.parameters.items()
        )
        _parameters.update(parameters)

        # set choice parameters
        if default:
            choices = (choices, default)
        _parameters["choice"] = setup_parameter(choices)

        # func and names
        def func(choice):
            return machines[choice]
            # update machine i/os
            # return update_machine_ios(machines[choice], meta_inputs, meta_outputs)

        name = "Metamachine({})".format(
            {ch: [machine.name for machine in machines[ch]] for ch in choices}
        )
        func.__name__ = name

        # make machine
        return cls(
            func, inputs=list(meta_inputs), output=list(meta_outputs), **_parameters
        )

    def solve(self, parameters):
        """solve metamachine: return list of machines"""

        # metamachine parameters
        param_values = solve_parameters(self.meta_parameters, parameters)

        # get the machines: run func
        fparams = self._func_signature
        fargs = []
        for name in fparams:
            if not name in param_values:
                breakpoint()
                raise ValueError("Invalid argument: %s" % name)
            fargs.append(param_values[name])

        # list of machines
        try:
            machines = self.func(*fargs)
        except Exception as exc:
            LOGGER.info(f"Could not solve metamachine '{self}': {repr(exc)}")
            raise exc

        if not isinstance(machines, (list, tuple)):
            machines = [machines]

        # solve sub machines recursively
        solved = []
        for machine in machines:
            _solved, _params = machine.solve(parameters)
            solved.extend(_solved)
            param_values.update(_params)

        # update machine i/os
        solved = update_machines_ios(solved)
        return solved, param_values

    def recall(self, history):
        """solved metamachines from history"""
        return MetaTask.deserialize(self, history)


#
# utilities


def autorun(
    machines,
    indices=None,
    branches=None,
    output_indices=None,
    output_branches=None,
    fallback=True,
    **kwargs,
):
    # run parameters: write mode
    mode = kwargs.pop("mode", None)
    if kwargs.pop("overwrite", False):
        mode = "overwrite"
    # other
    callback = kwargs.pop("callback", None)
    dry = kwargs.pop("dry", False)

    # make tasks
    graph = DependencyGraph.generate(
        machines, indices, branches, output_indices, output_branches, **kwargs
    )
    # run graph
    graph.run(mode=mode, callback=callback, dry=dry, fallback=fallback)
    return graph.tasks


def replay(machines, history, meta=None, **kwargs):
    """replay history"""
    graph = DependencyGraph.recall(machines, history, meta=meta)
    graph.run(**kwargs)
    return graph.tasks


def dispatch_parameters(identifiers, parameters):
    """return dict of per-id parameters"""
    if set(identifiers) & set(parameters):
        if not set(identifiers) <= set(parameters):
            raise ValueError(f"Missing identifiers in id-wise parameter")
        return parameters

    return {id: parameters for id in identifiers}


def update_machines_ios(machines):
    """set intermediary machine i/os to temporary"""
    meta_inputs, meta_outputs = get_meta_ios(machines)
    updated = []
    for machine in machines:
        inputs = dict(machine.inputs)
        for name, alts in inputs.items():
            alts = list(alts)
            for i, input in enumerate(alts):
                if input in meta_inputs:
                    continue
                elif not input.temp:
                    alts[i] = input.update(temp=True)
            inputs[name] = alts

        outputs = dict(machine.outputs)
        for name, alts in outputs.items():
            alts = list(alts)
            for i, output in enumerate(alts):
                if output in meta_outputs:
                    continue
                elif not output.temp:
                    alts[i] = output.update(temp=True)
            outputs[name] = alts

        # update machine
        updated.append(machine.copy(inputs=inputs, outputs=outputs))
    return updated
