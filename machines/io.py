# -*- coding: utf-8 -*-
""" machine's inputs/outputs """

import re
import itertools
from . import target
from .handlers import BaseFileHandler


class TargetType:
    """input/output object"""

    item_re = re.compile(r"^[\w]+$")

    def __init__(self, dest, type=None, *, handler=None, temp=False):
        """init IO object

        Parameters
        ===
            dest: i/o destination in storage
                use Ellipsis (...) for variable i/o
            type: assign i/o type
                for selecting storage by i/o type
            handler: FileHandler
                set file handler for the targets
            temp: [False]/True
                set target's temp flag to True

        """
        if dest is Ellipsis:
            # undefined I/O
            pass
        else:
            self._check_obj_type("dest", dest)

        if type:
            self._check_obj_type("type", type)

        if handler and not isinstance(handler, BaseFileHandler):
            raise TypeError(f"Invalid FileHandler object: {handler}")

        self.dest = dest
        self.type = type
        self.handler = handler
        self.temp = temp

    @property
    def is_virtual(self):
        return self.dest is Ellipsis

    def __eq__(self, other):
        """object compare"""
        if other is None:
            return False
        elif not isinstance(other, type(self)):
            raise TypeError(
                "Invalid comparison with object of type: %s" % str(type(other))
            )
        return self.dest == other.dest

    def __hash__(self):
        return hash(self.dest)

    def __repr__(self):
        cls = type(self).__name__
        repr = self.dest
        if self.type:
            repr += f", {self.type}"
        return f"{cls}({repr})"

    def __str__(self):
        return str(self.dest)

    def update(self, **kwargs):
        """return new input with updated info"""
        attrs = {
            "dest": self.dest,
            "type": self.type,
            "handler": self.handler,
            "temp": self.temp,
        }
        attrs.update(**kwargs)
        return self.__class__(**attrs)

    def target(self, index=None, branch=None, **kwargs):
        """generate target for given id/branch"""

        if self.is_virtual:
            raise RuntimeError("Cannot generate target from undefined i/o")

        return target.Target(
            self.dest,
            index,
            branch,
            type=self.type,
            handler=self.handler,
            temp=self.temp,
            **kwargs,
        )

    def targets(self, identifiers, **kwargs):
        """return list of targets"""
        return [self.target(index, branch, **kwargs) for index, branch in identifiers]

    @classmethod
    def _check_obj_type(cls, name, value):
        try:
            assert cls.item_re.match(value)
        except AssertionError:
            raise ValueError(f"Invalid value for TargetType's {name}: '{value}'")
        except TypeError:
            raise TypeError(f"Invalid type for TargetType's {name}: '{value}'")


# output and input target types
Output = TargetType
Input = TargetType

import warnings


def TaskIO(TargetType):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "TaskIO class is deprecated, use TargetType instead", DeprecationWarning
        )
        super().__init__(*args, **kwargs)


def parse_io(obj, allow_alts=True):
    """parse I/O expression

    Equivalent syntaxes:
        {"A": Input("A"), "B": Input("B")}
        [Input("A""), Input("B")]
        ["A", "B"]
        "A & B",

    Input's type and destination
        "A:T" == {"A": Input("A", type="T")}
        "A::D" == {"A": Input("D")}
        "A:T:D" == {"A": Input("D", type="T")}

    Alternative targets:
        "A|B" == {"A": [Input("A"), Input("B")]}
        "A::A1 | A::A2" == {"A": [Input("A1"), Input("A2")]}

    """
    if not obj:
        # no i/o
        return {}

    elif isinstance(obj, TargetType):
        return {obj.dest: [obj]}

    elif isinstance(obj, str):
        ios = {}
        for part in obj.split("&"):
            for name, _ios in parse_alt_ios(part).items():
                ios.setdefault(name, []).extend(_ios)

    elif isinstance(obj, list):
        ios = {}
        for item in obj:
            if isinstance(item, TargetType):
                ios.setdefault(item.dest, []).append(item)

            elif isinstance(item, str):
                for name, _ios in parse_alt_ios(item).items():
                    ios.setdefault(name, []).extend(_ios)
            else:
                raise ValueError(f"Invalid i/o list expression: {obj}")

    elif isinstance(obj, dict):
        ios = {}
        for name in obj:
            item = obj[name]
            if isinstance(item, list):
                # list of ios
                if not all(isinstance(io, TargetType) for io in item):
                    raise ValueError(f"Invalid i/o dict-of-list expression: {obj}")
                ios[name] = item

            elif isinstance(item, TargetType):
                # single io
                ios[name] = [item]
            else:
                raise ValueError(f"Invalid i/o dict expression: {obj}")

    else:
        raise ValueError(f"Invalid i/o expression: {obj}")

    if not allow_alts and any(len(items) > 1 for items in ios.values()):
        raise ValueError(f"Alternative i/o are not allowed: {obj}")

    return ios


def parse_alt_ios(expr):
    ios = {}
    name = None
    for part in expr.split("|"):
        alt, io = parse_string_io(part)
        if name is None:
            name = alt
        elif alt != io.dest and alt != name:
            raise ValueError(f"Cannot have multiple names for alternative i/os: {part}")
        ios.setdefault(name, []).append(io)
    return ios


def parse_string_io(expr):
    """parse single i/o from string"""

    # split at ":"
    iostr = expr.split(":")
    items = {
        key: val for key, val in zip(["name", "type", "dest"], iostr) if val.strip()
    }

    # solve name
    name = items["name"].split("|")[0].strip()

    # solve type
    if "type" in items:
        type = items["type"].strip()
    else:
        type = None

    # solve dest (check for alternative/secondary targets)
    if "dest" in items:
        dest = items["dest"].strip()
    else:
        dest = name

    # make TargetType
    return name, TargetType(dest, type=type)
