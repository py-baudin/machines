# -*- coding: utf-8 -*-
""" toolbox class """

import logging
import collections

from .machine import Machine
from machines.decorators import machine, metamachine
from .parameters import Parameter
from .io import TargetType
from .handlers import FileHandler
from . import cli, utils

# TODO: program groups / program searching

LOGGER = logging.getLogger(__name__)


class UnknownProgram(Exception):
    pass


class Toolbox:
    """dict-like collection of programs"""

    def __init__(self, name, description=None, default_handler=None):
        """initialize a toolbox with a name"""
        self.name = name
        self.description = description
        self.programs = collections.OrderedDict()
        self.programs_help = {}
        self.programs_manual = {}
        self.meta = {}  # program meta data
        self.groups = {}  # program group
        self.default_handler = default_handler  # default file handler
        self.handlers = {}  # store file handlers here for convenience
        self.initializers = []
        self.comparators = {}  # store data comparators here for convenience
        self.signature = None

    @property
    def machines(self):
        """return machines"""
        return list(self.programs.values())

    @property
    def relationships(self):
        """return relationships between machines"""
        return get_relationships(self.programs)

    def __getitem__(self, name):
        """get program by name"""
        if not name in self.programs:
            raise UnknownProgram("Unknown program: %s" % name)
        return self.programs[name]

    def __contains__(self, name):
        return name in self.programs

    def __iter__(self):
        """iterate programs"""
        return iter(self.programs)

    def reset_program(self, name, machine):
        """replace program"""
        if isinstance(machine, Machine):
            pass
        elif isinstance(machine, (dict, list)):
            machine = metamachine(machine)
        else:
            raise TypeError(f"Invalid machine: {machine}")
        self.programs[name] = machine

    def add_program(self, name, machine, help=None, manual=None, meta=None, group=None):
        """add a new machine (or sequence of) to the box"""
        if name in self.programs:
            raise ValueError("Machine %s already added" % name)

        LOGGER.info("Adding new program: %s", name)
        self.reset_program(name, machine)

        # store program
        self.programs_help[name] = help
        self.programs_manual[name] = manual

        # meta
        if not meta:
            meta = {}
        elif not isinstance(meta, dict):
            raise ValueError("'meta' value must be a dict")
        self.meta[name] = meta

        # group
        self.groups.setdefault(group, []).append(name)

    def remove_programs(self, programs):
        """remove programs"""
        if isinstance(programs, str):
            programs = [programs]
        for prog in programs:
            self.programs.pop(prog)
            self.programs_help.pop(prog)
            self.programs_manual.pop(prog)
            self.meta.pop(prog, None)
            for group in self.groups.values():
                if prog in group:
                    group.remove(prog)

    def add_handler(self, target, handler, replace=False):
        """add file handler"""
        if not isinstance(handler, FileHandler):
            raise TypeError(f"Invalid file handler type: {handler}")

        if not replace and target in self.handlers:
            raise ValueError(f"File handler already set for target {target}")
        self.handlers[target] = handler

    def add_handlers(self, handlers={}, **kwargs):
        """add file handlers"""
        for target, handler in {**handlers, **kwargs}.items():
            self.add_handler(target, handler)

    def add_initializer(self, initializer):
        """set a toolbox modifier/initializer"""
        if not isinstance(initializer, Modifier):
            raise TypeError()
        self.initializers.append(initializer)

    def add_comparators(self, comparators):
        """add file handlers"""
        self.comparators.update(comparators)

    def add_signature(self, filename, **items):
        """add signature file to output directories"""
        self.signature = utils.Signature(filename, **items)

    def cli(self, args=None):
        """shortcut to create a cli session"""
        _cli = cli.setup(self)
        _cli(args)

    @property
    def info(self):
        """toolbox info"""
        return {
            "name": self.name,
            "programs": [
                {
                    "name": name,
                    "description": self[name].description,
                    "aggregate": self[name].aggregate,
                    "meta": self.meta[name],
                }
                for name in self.programs
            ],
            "groups": self.groups,
            "description": self.description,
        }


def get_relationships(machines):
    """generate dict of (name, machines) pairs
    ie. for a given output's name, collect all machines with name as output
    """
    if not isinstance(machines, dict):
        machines = {machines.name: machine for machine in machines}

    rel = {}
    for name, machine in machines.items():
        if name in rel:
            raise ValueError("Duplicate name: %s" % name)

        # add machine's name to outputs
        rel[name] = [machine]

        # machine's output
        for output in machine.outputs:
            if not output:
                continue
            elif output in rel:
                # add machine's output to outputs
                rel[output].append(machine)
            else:
                # add machine's output to outputs
                rel[output] = [machine]
    return rel


def modifier(**parameters):
    """decorator to create modifiers"""

    def decorator(func):
        return Modifier(func, **parameters)

    return decorator


class Modifier:
    """define modifier (intializer, etc) decorator"""

    def __repr__(self):
        return f"Modifier({self.func.__name__})"

    def __init__(self, func, **parameters):
        self.func = func
        self.parameters = {}
        for name, param in parameters.items():
            if not isinstance(param, Parameter):
                raise TypeError(f"Invalid parameter type: {param}")
            self.parameters[name] = param
        self.types = parameters
