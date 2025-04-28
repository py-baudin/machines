# -*- coding: utf-8 -*-
""" machines parameters """

import os
import abc
import collections
import pathlib
import json
import yaml
import logging
from .io import TargetType, parse_string_io
from .common import ParameterError

LOGGER = logging.getLogger(__name__)


def setup_parameter(obj=None, name=None, **kwargs):
    """helper function for creating a parameter object

    Args:
        obj: Parameter, ParameterType, type, list of values/types
        name: Parameter's name (optional)
        is_flag: setup a boolan flag Parameter (defaults to False)

        Parameter's keywords:
            default: default value (makes parameter optional)
            none: is None value accepted?
            nargs: (int) number of values, -1 for any-length sequence
            help/description: parameter's documentation

    Returns:
        Parameter object
    """
    if isinstance(obj, Parameter):
        return obj

    help = kwargs.pop("help", kwargs.pop("description", None))
    default = kwargs.pop("default", ...)

    # backward compatibility: type=(param_type, default)
    if isinstance(obj, tuple):
        # todo: deprecation warning
        type = obj[0]
        default = obj[1]  # overwrite default value
    elif isinstance(obj, list) and len(set(obj)) == 1:
        # todo: deprecation warning
        type = obj[0]
        kwargs["nargs"] = len(obj)
    else:
        type = obj

    # special types: flag/switch
    if kwargs.pop("is_flag", False):
        # flag
        default = False if default is Ellipsis else default
        type = Flag(enable=name)
        # ignore type, nargs and none
        return Parameter(type, name=name, default=default, help=help)

    elif isinstance(type, dict):
        # switch
        type = Switch(type)
        # ignore type, nargs and none
        return Parameter(type, name=name, default=default, help=help)

    # else
    return Parameter(type, name=name, default=default, help=help, **kwargs)


def setup_parameter_type(type):
    """helper function to set parameter type from simple objects

    type == None -> string parameter type
    type = type1 -> TYPE1 parameter type
    type == [type1, ...] -> multi-type parameter type
    type == [value1, value2] -> Choice parameter type

    """
    if isinstance(type, ParameterType):
        # already set
        pass

    elif type is None:
        type = STRING

    elif isinstance(type, list):
        if all(item in BASE_TYPES for item in type):
            # multi-type short cut
            type = BaseType(*type)

        elif not any(
            item in BASE_TYPES or isinstance(item, ParameterType) for item in type
        ):
            # choice shortcut
            type = Choice(type)
    elif type in BASE_TYPES:
        type = BASE_TYPES[type]

    else:
        raise ValueError(f"Invalid parameter type: {type}")
    return type


def setup_variable_io(
    dest,
    type=None,
    handler=None,
):
    """return VariableIO parameter type"""
    if dest is Ellipsis or isinstance(dest, str):
        return VariableIO(type=type, handler=handler)
    elif isinstance(dest, (list, tuple, dict)):
        return VariableSelector(dest, type=type, handler=handler)
    else:
        raise ValueError(f"Invalid value for `dest`: {dest}")


def solve_parameters(parameters, values):
    """replace parameter objects with their values from `values`"""
    solved = {}
    for name, parameter in parameters.items():
        if not isinstance(parameter, Parameter):
            raise TypeError(f"Expected Parameter object, got: {parameter}")

        elif isinstance(parameter.type, Freeze):
            # freeze
            solved[name] = parameter.type.value
        else:
            # parse/cast value
            solved[name] = parameter(values.get(name, ...))
    return solved


class ParameterType(abc.ABC):
    """Base class for Parameter types"""

    flags = None  # return dict of flags where applicable

    def __call__(self, value):
        return self.convert(value)

    @abc.abstractmethod
    def convert(self, value):
        """convert value"""


class Parameter:
    """Parameter object"""

    @property
    def required(self):
        return self.default is Ellipsis

    @property
    def flags(self):
        return self.type.flags

    def __init__(
        self, type, *, name=None, nargs=None, default=..., none=False, help=None
    ):
        self.type = setup_parameter_type(type)
        self.name = name
        self.none = none or default is None
        self.nargs = nargs
        self.help = help
        if default is not Ellipsis:
            self.default = self.parse(default)
        else:
            self.default = ...

    def __call__(self, *args, **kwargs):
        return self.parse(*args, **kwargs)

    def parse(self, value=...):
        """convert parameter value"""
        if value is Ellipsis:
            if self.default is not Ellipsis:
                return self.default
            raise ParameterError(f'Missing required parameter: "{self.name}"')

        elif value is None:
            if self.none:
                return None
            raise ParameterError(f"Parameter `{self.name}` cannot be None")

        elif self.nargs is None:
            return self.type(value)

        elif self.nargs > 0:
            if not isinstance(value, collections.abc.Sequence):
                raise ParameterError(
                    f"Expected {self.nargs} values for parameter {self.name}"
                )
            elif len(value) != self.nargs:
                raise ParameterError(
                    f"Expected {self.nargs}!={len(value)} for parameter {self.name}"
                )

        elif self.nargs in (-1, 1) and not isinstance(value, collections.abc.Sequence):
            # if nargs in (-1, 1) and value is a scalar, convert to a list
            value = [value]

        # else: multiple values
        seqtype = type(value)
        return seqtype(self.type(item) for item in value)

    def __eq__(self, other):
        attrs = (self.type, self.nargs, self.default, self.none)
        return attrs == (other.type, other.nargs, other.default, other.none)

    def __str__(self):
        if not self.nargs:
            return f"{self.type}"
        elif self.nargs > 0:
            return ",".join([str(self.type)] * self.nargs)
        else:
            return f"{self.type}*"

    def __repr__(self):
        return f"Parameter({self.type}, name={self.name}, default={self.default}, nargs={self.nargs})"

    @property
    def info(self):
        """return a dict representation"""
        info = {
            "type": self.type,
            "name": self.name,
            "default": self.default,
            "nargs": self.nargs,
            "none": self.none,
            "required": self.required,
            "flags": self.flags,
            "help": self.help,
        }
        return info


# parameter types


class BaseType(ParameterType):
    def __init__(self, type, *types, name=None):
        self.types = (type,) + tuple(types)
        if name is None:
            if len(types) == 1:
                self.name = str(self.types[0].__name__)
            else:
                self.name = "/".join(map(str, self.types))
        else:
            self.name = name

    def convert(self, value):
        for type in self.types:
            try:
                return type(value)
            except ValueError as exc:
                pass
        raise ParameterError(f"Invalid value type: {value}")

    def __repr__(self):
        return self.name


STRING = BaseType(str, name="STRING")
BOOL = BaseType(bool, name="BOOL")
INT = BaseType(int, name="INT")
FLOAT = BaseType(float, name="FLOAT")

BASE_TYPES = {
    "str": STRING,
    str: STRING,
    "STRING": STRING,
    "int": INT,
    int: INT,
    "INT": INT,
    "float": FLOAT,
    float: FLOAT,
    "FLOAT": FLOAT,
    "bool": BOOL,
    bool: BOOL,
    "BOOL": BOOL,
}

# choice


class Choice(ParameterType):
    """A multiple choice parameter"""

    def __init__(self, values):
        if len(values) < 1:
            raise ValueError("A Choice must have at least two values")
        if isinstance(values, dict):
            self.flags = values
            values = tuple(values.values())
        elif isinstance(values, collections.abc.Sequence):
            values = tuple(values)
        else:
            raise ValueError("A Choice must be a sequence/dict of items")
        self.values = values

    def convert(self, value):
        if not value in self.values:
            raise ParameterError(f"Value {value} is not among {self.values}")
        return value

    def __repr__(self):
        return f'Choice([{", ".join(map(str, self.values))}])'


# flags and switches
class Flag(ParameterType):
    """boolean flag"""

    def __init__(self, enable=None, disable=None):
        self.enable = enable
        self.disable = disable
        self.flags = {}
        if enable is not None:
            self.flags[enable] = True
        if disable is not None:
            self.flags[disable] = False

    def convert(self, value):
        if isinstance(value, str):
            try:
                return {"true": True, "1": True, "false": False, "0": False}[
                    value.lower()
                ]
            except KeyError:
                raise ParameterError(f"Invalid flag value: {value}")
        return bool(value)

    def __repr__(self):
        enable = [] if self.enable is None else [f"enable={self.enable}"]
        disable = [] if self.disable is None else [f"disable={self.disable}"]
        args = ", ".join(enable + disable)
        return f"Flag({args})"


class Switch(ParameterType):
    """multi-value switch"""

    def __init__(self, dct=None, **values):
        if dct:
            values = {**dct, **values}
        if not values:
            raise ValueError("A Switch must be initialized with at least one value")
        self.values = values
        self.flags = {key: key for key in values}

    def convert(self, value):
        try:
            return self.values[value]
        except KeyError:
            raise ParameterError(f"Invalid switch option: {value}")

    def __repr__(self):
        args = ", ".join(f"{key}={value}" for key, value in self.values.items())
        return f"Switch({args})"


# Path/File
class Path(ParameterType):
    """A Path type"""

    def __init__(self, exists=False):
        self.exists = exists

    def convert(self, value):
        value = str(value).replace("\\", os.path.sep).replace("/", os.path.sep)
        path = pathlib.Path(value)
        if self.exists and not path.exists():
            raise ParameterError(f"Path: {path} does not exists")
        return str(path)

    def __repr__(self):
        return "Path"


# config
class Config(ParameterType):
    """A configuration/dictionary parameter"""

    class ConfigFile(dict):
        """a `dict` wrapper with a `filename` attribute"""

        filename = None

        def __init__(self, *args, filename=None, **kwargs):
            try:
                super().__init__(*args, **kwargs)
            except ValueError as exc:
                raise ParameterError(f'Expecting mapping, received: `{args[0]}`')
            self.filename = filename

    def __init__(self, presets=None, exts=[".yml", ".txt", ".json"]):
        self.exts = exts
        self.presets = self.load_presets(presets)

    def load_presets(self, presets):
        """load presets as dictionary"""
        if not presets:
            return {}
        elif isinstance(presets, dict):
            return presets
        elif pathlib.Path(presets).is_dir():
            # load presets
            _presets = {}
            for ext in self.exts:
                for file in pathlib.Path(presets).glob(f"*{ext}"):
                    _presets[file.stem] = self.load(file)
            return _presets
        else:
            raise ValueError(f"Invalid `presets`: {presets}")

    def load(self, file):
        """load preset from file"""
        with open(file, "r") as fp:
            data = fp.read()
        try:  # YAML
            config = yaml.safe_load(data)
            return self.ConfigFile(config, filename=file)
        except Exception as exc:
            pass

        try:  # JSON
            config = json.loads(data)
            return self.ConfigFile(config, filename=file)
        except json.decoder.JSONDecodeError as exc:
            pass
        raise OSError(f"Invalid configuration file: {file}")

    def convert(self, value):
        presets = self.presets
        if str(value) in presets:
            return presets[str(value)]
        if isinstance(value, dict):
            return self.ConfigFile(value)
        elif pathlib.Path(value).is_file():
            return self.load(value)
        elif isinstance(value, str):
            return self.ConfigFile(yaml.safe_load(value))
        else:
            raise ParameterError(f"Invalid configuration file or value: {value}")

    def __repr__(self):
        presets = "[" + ", ".join(self.presets) + "]"
        return f"Config(presets={presets})"


# Variable input/output
class VariableIO(ParameterType):
    """Parameter Type to setup a variable TargetType (Input/Output)"""

    def __init__(self, *, type=None, handler=None):
        self.default_type = type
        self.default_handler = handler

    def convert(self, value):
        """return TargetType"""
        if isinstance(value, TargetType):
            return value
        _, target_type = parse_string_io(value)
        if target_type.type is None:
            if self.default_type:
                target_type.type = self.default_type
            elif target_type.handler is None:
                target_type.handler = self.default_handler
        return target_type

    def __repr__(self):
        return "Variable I/O"


class VariableSelector(VariableIO):
    """Setup fixed choice of variable TargetType"""

    def __init__(self, obj, *, type=None, handler=None):
        choice, flags = {}, {}

        if isinstance(obj, (list, tuple)):
            for dest in obj:
                if not isinstance(dest, TargetType):
                    dest = TargetType(dest, type=type, handler=handler)
                choice[dest.dest] = dest

        elif isinstance(obj, dict):
            for name, dest in obj.items():
                if not isinstance(dest, TargetType):
                    dest = TargetType(dest, type=type, handler=handler)
                elif not isinstance(name, str):
                    raise ValueError(f"Invalid value name: {name}")
                flags[name] = name
                choice[name] = dest

            # setup flags names
            self.flags = flags
        else:
            raise ValueError(f"Invalid value for `dest`: {dest}")

        self.choice = choice

    def convert(self, value):
        if not value in self.choice:
            raise ParameterError(f"Invalid option value: {value}")
        return self.choice[value]

    def __repr__(self):
        return f"Variable I/O (choices: {list(self.choice)})"


class Freeze(ParameterType):
    """Freeze parameter value"""

    def __init__(self, value):
        self.value = value

    def convert(self, value):
        return self.value

    def __repr__(self):
        return f"Frozen(type(self.value))"
