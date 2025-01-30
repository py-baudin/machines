""" misc utilities """

# -*- coding: utf-8 -*-
from collections import abc
import pathlib
from functools import cmp_to_key
import json
import os
import datetime
import hashlib
import getpass
import logging

from .common import SEP_1, SEP_2, SEP_FLAT, DELIM, NULL_ID
from . import version
import warnings

LOGGER = logging.getLogger(__name__)


def id_repr(values):
    """represent id or branch"""
    return id_to_string(values)


def target_repr(
    name,
    index=None,
    branch=None,
    version=None,
    sep1=SEP_1,
    sep2=SEP_2,
    noindex="",
    nobranch=SEP_2,
):
    """represent target"""
    if not index:
        repr = noindex
    else:
        repr = str(index)

    # add name
    repr += sep1 + name

    if not branch:
        repr += nobranch  # if empty branch
    else:
        repr += sep2 + str(branch)

    if version is not None:
        repr += f"(v{version})"
    return repr


def identifier_repr(index=None, branch=None, sep2=SEP_2, noindex="", nobranch=SEP_2):
    """represent index/branch"""

    if not index:
        repr = noindex
    else:
        repr = id_repr(index)

    if not branch:
        repr += nobranch  # if empty branch
    else:
        repr += sep2 + id_repr(branch)
    return repr


def obj_repr(name, index=None, branch=None, **params):
    """represent object with name/id/branch and parameters"""
    strmap = lambda k, v: ("{}={:.22}[...])" if len(str(v)) > 25 else "{}={}").format(
        str(k), repr(v)
    )
    strrepr = name
    if params:
        strrepr += "({})".format(", ".join([strmap(*p) for p in params.items()]))
    if index or branch:
        strrepr += "[{}]".format(identifier_repr(index, branch))
    return strrepr


def task_repr(task):

    if task.aggregate:
        strio = croplist([f"[{croplist(input)}]" for input in task.inputs])
    else:
        strio = croplist(task.inputs)
    if task.output:
        strio += "->" + str(task.output)
    return f"{task.name}({strio})"


def croplist(seq, maxio=3):
    """represent cropped list"""
    if len(seq) > maxio:
        seq = seq[:maxio] + ["..."]
    return ", ".join([str(item) for item in seq])


def id_to_string(id, sep=SEP_FLAT, delim=DELIM, nodelim=True, none=NULL_ID):
    """convert id to string"""
    if id is None:
        return none

    elif isinstance(id, str):
        # single value
        return id.strip()

    elif isinstance(id, tuple):
        # multiple values

        string = sep.join([id_to_string(item, nodelim=False) for item in id])
        if nodelim:
            # return as is
            return string
        # or add delimiters
        return delim[0] + string + delim[1]

    else:
        raise ValueError(f"Invalid id type: {id}")


def id_from_string(string, sep=SEP_FLAT, delim=DELIM, none=NULL_ID):
    """convert id from string"""
    string = string.strip("")

    if string == none:
        return None

    elif not string:
        raise ValueError(f"Invalid string-id: '{string}'")

    elif string[0] == delim[0] and string[-1] == delim[-1]:
        string = string[1:-1]

    if not sep in string:
        return string

    elif not delim[0] in string:
        return tuple(string.split(sep))

    id = []
    iprev = 0
    dcount = 0
    for i, c in enumerate(string):
        if c == sep and dcount == 0:
            # separator
            id.append(id_from_string(string[iprev:i]))
            iprev = i + 1
        elif c == delim[0]:
            # open delimiter
            dcount += 1
        elif c == delim[1]:
            # close delimiter
            dcount -= 1

    if dcount != 0:
        raise ValueError(f"Bad id syntax in: {string}")

    # add remaining part to id
    id.append(id_from_string(string[iprev:]))

    return tuple(id)


def indices_as_key(task):
    return (task.index, task.branch)


def printer(message, id=None, *args, **kwargs):
    """print utility"""
    if isinstance(id, tuple):
        message = f"{identifier_repr(*id)}: {message}"
    if isinstance(id, list):
        strids = croplist([identifier_repr(*_id) for _id in id])
        message = f"{strids}: {message}"
    # _printer.info(message, *args, **kwargs)
    print(message)


def hash_file(filename):
    """generate file hash"""
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def as_string(obj):
    """recursive as-string function"""
    if isinstance(obj, dict):
        return {key: as_string(value) for key, value in obj.items()}
    elif isinstance(obj, (tuple, list)):
        return [as_string(item) for item in obj]
    return str(obj)


class Signature:
    """Signature generator
    Store a json file with custom info
    (typically: version, date, user, hash, toolbox' name, command line, etc.)

    """

    PRESETS = {
        "$DATETIME": lambda _: datetime.datetime.now().strftime("%Y%m%d-%H%M%S"),
        "$DATE": lambda _: datetime.datetime.now().strftime("%Y%m%d"),
        "$MACHINES": version.__version__,
        "$USER_LOGIN": getpass.getuser(),
        "$FILES": lambda dirname: [
            file.name for file in pathlib.Path(dirname).glob("*")
        ],
        "$HASH": lambda dirname: {
            file.name: hash_file(file) for file in pathlib.Path(dirname).glob("*")
        },
        "$DIRNAME": lambda dirname: dirname,
    }

    def __init__(self, filename, **items):
        self.filename = filename
        self.items = items

    def __call__(self, dirname):
        """store signature into dirname"""
        filename = pathlib.Path(dirname) / self.filename
        if not filename.parent.is_dir():
            warnings.warn(f"Directory {dirname} not found.")
            return
        elif filename.is_file():
            warnings.warn(f"Previous signature found at: {filename}.")
            os.remove(filename)

        # generate content
        content = {}
        for key, value in self.items.items():
            if isinstance(value, str) and value in self.PRESETS:
                value = self.PRESETS[value]

            if callable(value):
                try:
                    content[key] = value(dirname)
                except Exception as exc:
                    warnings.warn(f"Could not solve signature item: {key}")
                    LOGGER.info(exc)
            else:
                content[key] = value

        # store content
        with open(filename, "w") as fp:
            try:
                json.dump(as_string(content), fp)
            except Exception as exc:
                warnings.warn(f"Could not store signature file at: {filename}")
                LOGGER.info(exc)
