""" target converters """

# -*- coding: utf-8 -*-
import os
import re
import pathlib
import glob
import datetime
from itertools import cycle
from .target import Target, Branch, RE_ID_STRING, RE_TARGET_STRING
from .common import SEP_1, SEP_2, SEP_FLAT, SEP_DIR
from .utils import id_to_string, id_from_string

# separator names
SEPARATORS = ["sep_main", "sep_secondary", "sep_index", "sep_branch"]

# path for index == None
PATH_NONE = "_"

# Default configuration for workdir
WORKDIR_EXPR = {
    "struct": "<index>/<name><branch>",
    # "index": "<id>[/<id>]",
    "index": "<id>[.<id>]",
    "branch": "~<id>[.<id>]",
    "noindex": "_",
    "nobranch": "",
    "name": None,
}

# Default configuration for targetdirs
TARGETDIR_EXPR = {
    "struct": "<index><branch>",
    "index": "<id>[.<id>]",
    "branch": "~<id>[.<id>]",
    "noindex": "_",
    "nobranch": "",
    "name": None,  # required
}


class TargetConverter:
    """target to path converter"""

    def _to_path(self, target, new=False):
        pass

    def _from_path(self, path):
        pass

    def to_path(self, target, check=True, new=False):
        """converter target to path"""
        if not isinstance(target, Target):
            raise TypeError()

        # get path
        path = self._to_path(target, new=new)

        # check convert and back
        if check and self._from_path(path) != target:
            raise ValueError(f"Invalid target: '{target}'")

        return os.path.normpath(path)

    def from_path(self, path, check=True):
        """converter path to target"""

        # normalize path
        path = "/".join(pathlib.Path(path).parts)
        if path == ".":
            path = ""

        # get target
        target = self._from_path(path)

        if not isinstance(target, Target):
            raise TypeError()

        # check convert and back
        if check and self._to_path(target) != path:
            raise ValueError("Invalid path: '%s'" % path)
        return target

# deprecated
class TargetToPath(TargetConverter):
    """standard target to path converter

    Target("A", index, branch) <-> id1/id2/...|name~br1·br2....}
    with:
        "|" = sep_main (actually, default is: "/" )
        "~" = sep_sec
        "/" = sep_index
        "·" = sep_branch
    """

    PATH_NONE = "_"  # path for no-index targets

    def __init__(
        self, sep_main=SEP_DIR, sep_sec=SEP_2, sep_index=SEP_DIR, sep_branch=SEP_FLAT
    ):
        self.sep_main = sep_main
        self.sep_sec = sep_sec
        self.sep_index = sep_index
        self.sep_branch = sep_branch

    def _to_path(self, target, **kwargs):
        """convert target to path"""
        name = target.name
        index = target.index
        branch = target.branch

        # index
        if not index:
            # _/name
            path = PATH_NONE
        elif index:
            # join ids
            path = id_to_string(index.values, sep=self.sep_index)

        # name
        path = self.sep_main.join([path, name])

        # branch
        if branch:
            # join branches
            path += self.sep_sec + id_to_string(branch.values, sep=self.sep_branch)
        return path

    def _from_path(self, path):
        """convert path to target"""

        # split path with main separator
        if not self.sep_main in path:
            raise ValueError(
                f"Main seperator ('{self.sep_main}') not found in path: {path}"
            )
        head, tail = path.rsplit(self.sep_main, 1)

        # split secondary separator
        if self.sep_sec in tail:
            name, tail = tail.split(self.sep_sec, 1)
        else:
            name = tail
            tail = ""

        # index
        if head == PATH_NONE:
            index = None
        elif not self.sep_index in head:
            index = head
        else:
            index = id_from_string(head, sep=self.sep_index)

        # branch
        if not tail:
            branch = None
        elif not self.sep_branch in tail:
            branch = tail
        else:
            branch = id_from_string(tail, sep=self.sep_branch)

        return Target(name, index, branch)


class VersionerInt:
    """basic versioner for int versions"""

    def to_version(self, value):
        return int(value)

    def new_version(self, previous=0):
        previous = 0 if not previous else int(previous)
        return previous + 1

    def from_version(self, value):
        return str(value)


class VersionerDate:
    """basic versioner for date versions"""

    def to_version(self, value):
        return datetime.datetime.strptime(value, "%Y%m%d_%H%M%S.%f")

    def new_version(self, previous=None):
        return datetime.datetime.now()

    def from_version(self, value):
        return value.strftime("%Y%m%d_%H%M%S.%f")

# deprecated
class TargetToPathWithVersion(TargetToPath):
    """TargetToPath with target version"""

    def __init__(self, root, versioner=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.root = root
        if not versioner:
            versioner = VersionerInt()
        self.versioner = versioner

    def _from_path(self, path):
        path, version = path.rsplit("_v", maxsplit=1)
        target = super()._from_path(path)
        target.version = self.versioner.to_version(version)
        return target

    def _to_path(self, target, new=False):
        path = super()._to_path(target)
        version = target.version
        if version is None and new:
            # use new version
            version = self._get_last_version(path)
            version = self.versioner.new_version(version)
        elif version is None:
            # use last version
            version = self._get_last_version(path)
            if version is None:
                version = self.versioner.new_version()
        # else: version is defined
        path += "_v%s" % self.versioner.from_version(version)
        return path

    def _get_last_version(self, path):
        """get last version of target"""
        existing = glob.glob(os.path.join(self.root, path + "_v*"))
        if not existing:
            return None
        versions = [path.rsplit("_v", 1)[1] for path in existing]
        # return last version
        return max([self.versioner.to_version(v) for v in versions])

# deprecated
class TargetToPathDedicated(TargetConverter):
    """target to path converter for dedicated storage ("at root")

    Target("A", index, branch) <-> root/id1/id2.....~br1·br2....
    with:
        "/" = index separator
        "·" = branch separator
        "~" = secondary separator
        (no main separator)

    A target with a different name will raise a ValueError
    """

    def __init__(
        self,
        name,
        branch=None,
        sep_sec=SEP_2,
        sep_index=SEP_FLAT,
        sep_branch=SEP_FLAT,
        sep_main=None,
    ):
        self.name = name
        self.branch = Branch(branch)
        self.sep_index = sep_index
        self.sep_branch = sep_branch
        self.sep_sec = sep_sec

    def _to_path(self, target, new=False):
        """convert target to path"""
        index = target.index
        branch = target.branch

        if target.name != self.name:
            raise ValueError(f"Invalid target's name: {target}")
        elif self.branch and branch != self.branch:
            raise ValueError(f"Invalid target's branch: {target}")

        if self.branch:
            branch = None

        if index:
            # join ids
            path = id_to_string(index.values, sep=self.sep_index)
        else:
            path = PATH_NONE

        if branch:
            # join branches
            path += self.sep_sec + id_to_string(branch.values, sep=self.sep_branch)
        return path

    def _from_path(self, path):
        """convert path to target"""
        # split secondary separator
        if self.sep_sec in path:
            head, tail = path.split(self.sep_sec, 1)
        else:
            head = path
            tail = ""

        # index
        if head == PATH_NONE:
            index = None
        elif not self.sep_index in head:
            index = head
        else:
            index = id_from_string(head, sep=self.sep_index)

        # branch
        if not tail:
            branch = None
        elif not self.sep_branch in tail:
            branch = tail
        else:
            branch = id_from_string(tail, sep=self.sep_branch)

        if self.branch:
            branch = self.branch + branch

        return Target(self.name, index, branch)


class TargetToPathExpr(TargetConverter):
    # authorized string characters
    targetchars = RE_TARGET_STRING

    def __init__(
        self,
        struct: str = "<index>/<name><branch>",
        index: str = "<id>[.<id>]",
        branch: str = "~<id>[.<id>]",
        noindex: str = "_",
        nobranch: str = "",
        values: dict = None,
        name: str = None,
        default_branch: str = None,
    ):
        """init expression-based TargetConverter

        Parameters
            struct: structure of the target-path
            index: structure of the index
            branch: strucuture of the branch
            noindex: index representation if None
            nobranch: branch representation if None
            name: if set, required target name
            values: dict of accepted values for index and branch structures
        """
        # check struct
        if not '<index>' in struct or not '<branch>' in struct:
            raise ValueError(f'Missing field <index> or <branch> in `struct`')
        if name is None and not '<name>' in struct:
            raise ValueError(f'Missing field <name> in `struct`')
        
        self.struct = struct
        self.name = name
        self.index = IdToPathExpr(index, noindex, values=values)
        self.branch = IdToPathExpr(branch, nobranch, values=values)
        self.default_branch = default_branch

    def __repr__(self):
        return f"struct={self.struct};index={self.index};branch={self.branch};name={self.name}"

    def _to_path(self, target, **kwargs):
        if self.name and target.name != self.name:
            raise ValueError(f"Unauthorized target's name: {target.name}")

        index = self.index.to_path(target.index)

        if self.default_branch:
            if target.branch not in (Branch(None), self.default_branch):
                raise ValueError(f"Unauthorized branch: {target.branch}")
            branch = self.branch.to_path(None)
        else:
            branch = self.branch.to_path(target.branch)

        path = (
            self.struct.replace("<name>", target.name)
            .replace("<index>", index)
            .replace("<branch>", branch)
        )
        return path

    def _from_path(self, path, **kwargs):

        regindex = rf"(?P<index>{re.escape(self.index.prefix)}.+?{re.escape(self.index.suffix)}|{re.escape(self.index.noid)})"
        regbranch = rf"(?P<branch>{re.escape(self.branch.prefix)}.+?{re.escape(self.branch.suffix)}|{re.escape(self.branch.noid)})"

        nameexpr = rf"[0-9a-zA-Z{re.escape(''.join(self.targetchars))}]+?"
        regname = rf"(?P<name>{self.name if self.name else nameexpr})"
        regex = (
            self.struct.replace("<index>", regindex)
            .replace("<name>", regname)
            .replace("<branch>", regbranch)
            + r"$"
        )
        match = re.match(regex, path)
        if not match:
            raise ValueError(f"Invalid path structure: {path}")

        # name
        if not "<name>" in self.struct:
            name = self.name
        else:
            name = match.group("name")
            if self.name and name != self.name:
                raise ValueError(f"Unauthorized target's name: {name}")

        # index
        if match.group("index"):
            index = self.index.from_path(match.group("index"))
        else:
            index = None

        # branch
        if self.default_branch:
            branch = Branch(self.default_branch)
        elif match.group("branch"):
            branch = self.branch.from_path(match.group("branch"))
        else:
            branch = None

        return Target(name, index, branch)


class IdToPathExpr:
    """convert index/branch to path and back"""

    # authorized id characters
    idchars = RE_ID_STRING
    regex_part = re.compile(r"<(\w+)>")
    regex_gen = re.compile(r"\[([^\[\]]+)\]")

    def __init__(self, expr: str = "<id>[.<id>]", noid: str = "", values: dict = None):
        self.noid = noid
        self.expr = expr
        self.values = values if values else {}

        npart = len(self.regex_part.findall(expr))
        # if not "<id>" in expr:
        if npart == 0:
            raise ValueError(f"Missing <.> elements in expr: {expr}")

        self.prefix = next(re.finditer(r"^[^\<\[]*", expr)).group()
        self.suffix = next(re.finditer(r"[^\>\]]*$", expr)).group()

        # search generative parts
        match = self.regex_gen.search(expr)
        if match:
            # generative part
            gen = match.group(1)
            head, tail = expr.split(match.group())
            if set("[]") & (set(head) | set(tail)):
                raise ValueError(
                    f"Cannot have multiple generative groups in expression: {expr}"
                )
            self.gen_str = gen
            parts = self.regex_part.search(gen)
            self.gen_vals = list(parts.groups())
            charset = set(gen.replace(parts.group(0), ""))
            self.idchars = list(set(self.idchars) - charset)

            # self.nhead = head.count("<id>")
            self.head_vals = self.regex_part.findall(head)
            self.head_str = head  # re.escape(head)
            # self.ntail = tail.count("<id>")
            self.tail_vals = self.regex_part.findall(tail)
            self.tail_str = tail  # re.escape(tail)

        elif not set("[]") & set(expr):
            # fixed length
            # self.nhead = expr.count("<id>")
            self.head_vals = self.regex_part.findall(expr)
            self.head_str = expr  # re.escape(expr)
            # self.ntail = 0
            self.tail_vals = {}
            self.tail_str = ""

            self.gen_str = ""
            self.gen_vals = {}
        else:
            raise ValueError(f"Invalid expression: {expr}")

    def __repr__(self):
        return self.expr

    @property
    def idexpr(self):
        """get authorized index characters"""
        return rf"[a-zA-Z0-9{re.escape(''.join(self.idchars))}]"

    def _validate(self, name, value):
        """validate id value"""
        if not name in self.values:
            # ignore validation if id not provided in values
            return
        if isinstance(self.values[name], list):
            # list of values
            if not value in self.values[name]:
                raise ValueError(
                    f"Invalid identifier <{name}>: {value} not in {self.values[name]}"
                )

        elif isinstance(self.values[name], str):
            # regular expression
            if not re.match(self.values[name], value):
                raise ValueError(
                    f"Invalid identifier <{name}>: {value} does not match {self.values[name]}"
                )

        else:
            raise ValueError(f"Invalid validation for id <{name}>: {self.values[name]}")

    def to_path(self, id, validate=True):
        if not id:
            # return custom path for an empty id
            return self.noid

        # else, if 'id' is not None

        nhead = len(self.head_vals)
        ntail = len(self.tail_vals)
        id_len = nhead + ntail
        if not self.gen_str and len(id) != id_len:
            raise ValueError(f"Invalid id length: {id} != {id_len}")
        elif len(id) < id_len:
            raise ValueError(f"Invalid id length: {id} < {id_len})")

        # head
        head_str = self.head_str
        for value, name in zip(id, self.head_vals):
            if validate:
                self._validate(name, value)
            head_str = head_str.replace(f"<{name}>", value, 1)

        # generative
        gen_str = ""
        for value in id[nhead : -ntail if ntail else None]:
            gen_str += self.gen_str
            for name in self.gen_vals:
                if validate:
                    self._validate(name, value)
                gen_str = gen_str.replace(f"<{name}>", value, 1)

        # tail
        tail_str = self.tail_str
        for value, name in zip(id[-ntail:], self.tail_vals):
            if validate:
                self._validate(name, value)
            tail_str = tail_str.replace(f"<{name}>", value, 1)

        str_id = head_str + gen_str + tail_str
        return str_id

    def from_path(self, path, validate=True):
        if path == self.noid:
            return None

        # head
        head_expr = "^" + re.escape(self.head_str)
        for name in self.head_vals:
            head_expr = head_expr.replace(f"<{name}>", rf"({self.idexpr}+)")
        head_match = re.search(head_expr, path)
        if not head_match:
            raise ValueError(f"Cannot parse path: {path}")
        head = list(head_match.groups())
        if validate:
            for name, value in zip(self.head_vals, head):
                self._validate(name, value)
        remain = path[head_match.end() :]

        # tail
        tail_expr = re.escape(self.tail_str) + "$"
        for name in self.tail_vals:
            tail_expr = tail_expr.replace(f"<{name}>", rf"({self.idexpr}+)")
        tail_match = re.search(tail_expr, remain)
        if not tail_match:
            raise ValueError(f"Cannot parse path: {path}")
        tail = list(tail_match.groups())
        if validate:
            for name, value in zip(self.tail_vals, tail):
                self._validate(name, value)
        remain = remain[: tail_match.start()]

        # generative
        gen_expr = self.gen_str.replace(".", r"\.").replace("+", r"\+")
        for name in self.gen_vals:
            gen_expr = gen_expr.replace(f"<{name}>", rf"({self.idexpr}+)")

        mid = []
        while remain:
            gen_match = re.search(gen_expr, remain)
            if not gen_match:
                raise ValueError(f"Cannot parse path: {path}")
            try:
                mid.append(gen_match.group(1))
            except IndexError:
                raise ValueError(f"Invalid path: {path}")
            remain = remain[gen_match.end() :]
        if validate:
            names = cycle(self.gen_vals)
            for value in mid:
                self._validate(next(names), value)

        # generate path
        return tuple(head + mid + tail)
