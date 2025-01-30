# -*- coding: utf-8 -*-
""" common objects and functions """
import os
from enum import Enum
from collections import namedtuple


class Status(Enum):
    """Task status"""

    ERROR = 0
    REJECTED = 1
    RUNNING = 2
    PENDING = 3
    SUCCESS = 4
    SKIPPED = 5
    NEW = 6


# (id, branch) pair
Identifier = namedtuple("Identifier", ["index", "branch"])

# directory-type separator
SEP_DIR = "/"
# flat-type separator
SEP_FLAT = "."
# primary separator
SEP_1 = "#"
# secondary separator
SEP_2 = "~"
# id delimiters
DELIM = "{}"
# string for id=None
NULL_ID = "_"


class RejectException(Exception):
    """Special exception for rejecting a task"""

    error = "Reject error"


class ExpectedError(Exception):
    """Special exception for a clean error display"""

    error = "Expected error"


class InvalidTarget(Exception):
    """Raise if target is not valid"""

    error = "Invalid target error"


class TargetAlreadyExists(Exception):
    pass


class TargetIsLocked(Exception):
    pass


class TargetDoesNotExist(Exception):
    pass


class ParameterError(Exception):
    """exception raised when a parameter's value is incorrect"""

    error = "Parameter error"
