# -*- coding: utf-8 -*-
""" targets """
import re
import itertools
from functools import total_ordering

from .common import Identifier
from .utils import id_repr, target_repr


# authorized target name values
RE_TARGET_STRING = ["+", "-", "_"]
RE_TARGET_REGEX = re.compile(rf"^[\w{re.escape(''.join(RE_TARGET_STRING))}]+$")

# authorized index/branch values
RE_ID_STRING = ["+", "-", "_", ":", "(", ")"]
RE_ID_REGEX = re.compile(rf"^[a-zA-Z0-9{re.escape(''.join(RE_ID_STRING))}]+$")


def targets(name, ids, **kwargs):
    """return one or several targets"""
    if not isinstance(ids, list):
        return Target(name, *ids, **kwargs)
    return [Target(name, *_ids, **kwargs) for _ids in ids]


@total_ordering
class Target:
    """Target class"""

    def __init__(
        self,
        name,
        index=None,
        branch=None,
        type=None,
        handler=None,
        version=None,
        attach=None,
        temp=False,
        task=None,
    ):
        """Target object"""
        if not isinstance(name, str):
            raise TypeError("Invalid target's type: '%s'" % name)
        elif not RE_TARGET_REGEX.match(name):
            raise ValueError("Invalid target's name: '%s'" % name)

        # store task (may be None)
        self.task = task

        # target's name
        self.name = name

        # target's identifiers
        self.index = Index(index)
        self.branch = Branch(branch)
        self.identifier = Identifier(self.index.values, self.branch.values)

        # target's type (if needed)
        self.type = type

        # target's handler (if needed)
        self.handler = handler

        # target's version (if needed)
        self.version = version

        # target is temporary
        self.temp = temp

        # target's attachment
        self._attachment = {}
        if attach:
            self.attach(attach)

    @property
    def signature(self):
        """target's signature"""
        return (self.index, self.name, self.branch)

    @property
    def attachment(self):
        """target's attachment"""
        return self._attachment

    @property
    def graph(self):
        if not self.task:
            raise RuntimeError("Target's task is not set")
        return self.task.graph

    @property
    def parents(self):
        """get parent targets"""
        return self.graph.get_parents(self)

    # @property
    # def factory(self):
    #     """return target's factory if any"""
    #     return self.task.factory

    # @property
    # def storage(self):
    #     """return target's storage"""
    #     return self.factory.get_storage(self)

    @property
    def temporary(self):
        """return True if target is stored in temporary storage"""
        return self.temp
        # return self.storage.temporary

    @property
    def location(self):
        """return storage location (is any)"""
        return self.storage.location(self)

    def attach(self, *args, overwrite=False, **kwargs):
        """attach information to the target (no overwrite)"""
        for attachment in args + (kwargs,):
            for key in attachment:
                value = attachment[key]
                if not overwrite and key in self.attachment:
                    # key already exists
                    raise ValueError(
                        f"Key '{key}' already in attachment with a different value"
                    )
                else:
                    # key does not exist
                    self._attachment[key] = value

    def update(self, name=None, index=..., branch=..., type=..., handler=...):
        """return modified target"""
        if name is None:
            name = self.name
        if index is ...:
            index = self.index
        if branch is ...:
            branch = self.branch
        if type is ...:
            type = self.type
        if handler is ...:
            handler = self.handler
        return self.__class__(
            name, index, branch, type, handler=handler, attach=self._attachment.copy()
        )

    def match(self, name, index=None, branch=None):
        """return True if ids match input's"""
        if "*" in name:
            regex = "^{}$".format(name.replace("*", ".*"))
            if not re.search(regex, self.name):
                return False
        elif name != self.name:
            return False
        # check indices
        return self.index.match(index) and self.branch.match(branch)

    def copy(self):
        """copy target (without attachment)"""
        return self.update()

    def serialize(self):
        """return target attributes in a serializable format"""
        return {
            "name": self.name,
            "index": self.index.values,
            "branch": self.branch.values,
        }

    @classmethod
    def deserialize(cls, name, index, branch, **kwargs):
        """deserialize target info"""

        def astuple(id):
            if not isinstance(id, list):
                return id
            return tuple(astuple(v) for v in id)

        index = astuple(index)
        branch = astuple(branch)
        return cls(name, index, branch, **kwargs)

    def __eq__(self, other):
        """check equality"""
        return self.signature == other.signature

    def __ne__(self, other):
        """inequality"""
        return self.signature != other.signature

    def __gt__(self, other):
        return self.signature > other.signature

    def __hash__(self):
        """target hash"""
        return hash(self.signature)

    def __repr__(self):
        """string representation"""
        return self.to_string()

    def to_string(self, **kwargs):
        """convert target to string
        options: sep1, sep2, noindex, nobranch, version
        """
        version = kwargs.pop("version", self.version)
        return target_repr(
            self.name, self.index, self.branch, version=version, **kwargs
        )


@total_ordering
class IdBase:
    """Base class for identifiers (index and branch)

    contatenation:
        Id1 + Id2 = (Id1, Id2)
    sub-ids:
        (Id1, (Id1, Id2))
    """

    allow_duplicate = False
    none_is_greater = True

    def __init__(self, *objs):

        cls = type(self)
        single = len(objs) == 1

        if not objs:
            # empty
            values = []
        elif single and objs[0] is None:
            # None
            values = [None]
        elif single and isinstance(objs[0], str):
            # string
            value = objs[0].strip()
            values = [None] if not value else [value]
        elif single and isinstance(objs[0], int):
            # int -> convert to str
            values = [str(objs[0])]
        elif single and isinstance(objs[0], cls):
            # if an item is already a IdBase
            values = objs[0]._values
        elif single and isinstance(objs[0], tuple) and not objs[0]:
            # empty tuple
            values = []
        elif single and isinstance(objs[0], tuple) and bool(objs[0]):
            # item is a non-empty tuple: check and concatenate values
            values = [cls(item).values for item in objs[0]]
        elif not single:
            # check all values sequentially
            values = [cls(obj).values for obj in objs]
        else:
            raise TypeError("Invalid %s: %s" % (cls.__name__, str(objs)))

        # check values
        for value in values:
            if isinstance(value, str) and not RE_ID_REGEX.match(value):
                raise ValueError("Invalid value: %s" % str(value))

        # check duplicates
        if not self.allow_duplicate:
            # remove duplicate
            valueset = set()
            values = [
                value
                for value in values
                if not (value in valueset or valueset.add(value))
            ]

        if len(values) > 1 and None in values:
            raise ValueError(
                "Multi-valued %s must not contain null values: %s"
                % (cls.__name__, values)
            )
        self._values = tuple(value for value in values if value is not None)

    @property
    def values(self):
        if not self:
            return None
        elif len(self) == 1:
            return self._values[0]
        return self._values

    def crop(self, n=1):
        """crop n identifiers"""
        cls = type(self)
        if n > len(self):
            return None
        return cls(self._values[:-n])

    def match(self, other, match_null=True):
        """return true if value matches other ('*' wildcards accepted)"""
        if not self:
            # special case: empty value
            if match_null:
                # allows matching "*"
                return (not other) or (other == "*")
            # only match other == None or other == ""
            return not other
        elif not other:
            return False

        cls = type(self)
        if not isinstance(other, tuple):
            other = (other,)

        # join and compare
        str_self = "___".join(self)
        str_other = "___".join(other)

        regex = "^{}$".format(str_other.replace("*", ".*"))
        if not re.match(regex, str_self):
            return False

        return True

    def __len__(self):
        return len(self._values)

    def __bool__(self):
        return len(self) > 0

    def __str__(self):
        """pretty print"""
        return id_repr(self._values)

    def __repr__(self):
        cls = type(self)
        txt = "(%s)" % ", ".join(repr(v) for v in self._values)
        return cls.__name__ + txt

    def __add__(self, other):
        """concatenation"""
        cls = type(self)
        return cls(self._values + cls(other)._values)

    def __radd__(self, other):
        cls = type(self)
        return cls(other) + self

    def __eq__(self, other):
        other = type(self)(other)
        return self.values == other.values

    def __ne__(self, other):
        other = type(self)(other)
        return self.values != other.values

    def __gt__(self, other):
        other = type(self)(other)
        if self.values == other.values:
            return False
        elif other.values is None:
            return not self.none_is_greater
        elif self.values is None:
            return self.none_is_greater
        else:
            for a, b in itertools.zip_longest(
                self._values, other._values, fillvalue=""
            ):
                if a > b:
                    return True
                elif a < b:
                    return False
            return False

    def __iter__(self):
        return iter(self._values)

    def __contains__(self, value):
        return value in self._values

    def __getitem__(self, key):
        return self._values[key]

    def __hash__(self):
        return hash(self._values)


class Index(IdBase):
    """task's index version of the identifier class"""

    allow_duplicate = True


class Branch(IdBase):
    """task's branch version of the identifier class"""

    none_is_greater = False

    def __add__(self, other):
        cls = type(self)
        other = cls(other)._values
        return cls(self._values + tuple(v for v in other if not v in self._values))


def ravel_identifiers(indices=None, branches=None):
    """return index/branch pairs"""
    if not isinstance(indices, list):
        indices = [indices]
    if not isinstance(branches, list):
        branches = [branches]

    if (len(indices) == 1) or (len(branches) == 1):
        return [Identifier(*id) for id in itertools.product(indices, branches)]

    elif len(indices) == len(branches):
        return [Identifier(*id) for id in zip(indices, branches)]

    else:
        raise ValueError("Incompatible numbers of indices and branches")
