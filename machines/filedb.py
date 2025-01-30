# -*- coding: utf-8 -*-
""" file mapping """

import os
import time
import shutil
import pickle
import json
import tempfile
import pathlib
import logging

# from . import virtualdir
from .handlers import FileHandler, file_handler, pickle_handler
from .target import Target
from .targetpath import TargetConverter, TargetToPathExpr


LOGGER = logging.getLogger(__name__)


class FileDB:
    """dict-like object to/from file mapping"""

    def __init__(
        self, root, converter=None, handlers=None, default_handler=None, signature=None
    ):
        """Init FileMap
        Parameters
        ===
            root: root storage directory
            handlers: dict of FileHandlers
            converter: None or TargetConverter
        """
        self.root = pathlib.Path(root)

        # target-to-path converter
        if not converter:
            # default converter
            self.converter = TargetToPathExpr()

        elif isinstance(converter, TargetConverter):
            self.converter = converter

        else:
            # wrong type
            raise TypeError("Invalid target converter: %s" % converter)

        # target handlers
        handlers = make_handlers(handlers)

        # set default handler
        if not default_handler:
            default_handler = pickle_handler
        self.default_handler = handlers.pop("default", default_handler)
        self.handlers = handlers

        # signature
        self.signature = signature

    def __repr__(self):
        """represent file db"""
        name = type(self).__name__
        return f"{name}({self.root}, handlers={list(self.handlers)})"

    def __iter__(self):
        """return iterator on existing targets"""
        failed = []
        for path, dirs, files in os.walk(self.root):
            path = pathlib.Path(path)
            # remove tempdirs
            dirs[:] = [dir for dir in dirs if not dir.startswith(".")]

            if dirs or not files or all(file.startswith(".") for file in files):
                # skip if not leaf / no files / only temp files
                continue
            # else: leaf
            try:
                target = self.from_path(path)
            except (TypeError, ValueError) as exc:
                # skip target
                LOGGER.info("Skipping path %s: %s", path, exc)
                # failed.append((path, str(exc)))
                failed.append(path)
                continue
            yield target
        if failed:
            LOGGER.warn(f"Failed to parse {len(failed)} targets in '{self.root}'")
        return failed

    def __bool__(self):
        """check whether storage is empty"""
        return bool(os.listdir(self.root))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            raise TypeError(f"Cannot compare FileStorage to {type(other)}")
        return self.root == pathlib.Path(other.root)

    def __contains__(self, target):
        """check if target data exist"""
        try:
            path = self.to_path(target)
        except ValueError:
            return False
        return os.path.exists(path)

    def __getitem__(self, target):
        """load target data"""
        path = self.to_path(target)
        # check path
        if not os.path.exists(path):
            raise KeyError(f"Path '{path}' does not exist")
        # get file handler
        handler = self._get_handler(target)
        # read data
        data = handler.load(target, path)
        return data

    def __setitem__(self, target, value):
        """store targe data
        First save to temp dir then copy to destination
        """
        # as path, may be a new path
        path = self.to_path(target, new=True)

        # check path
        if os.path.exists(path):
            # target exists (overwrite)
            shutil.rmtree(path)

        # get file handler
        handler = self._get_handler(target)

        # write
        with tempfile.TemporaryDirectory() as tempdir:
            # first, put data in temp dir
            handler.save(target, tempdir, value)
            # add signature
            if self.signature:
                self.signature(tempdir)
            # copy to output path
            shutil.copytree(tempdir, path)

    def __delitem__(self, target):
        """remove target's data"""
        path = self.to_path(target)

        # check path
        if not os.path.exists(path):
            raise KeyError(f"Path '{path}' does not exist")

        shutil.rmtree(path)
        removedirs(path, root=self.root)

        try:
            os.rmdir(self.root)
        except OSError:
            # tempdir not empty
            pass

    def _get_handler(self, target):
        """get handler for provided target"""
        if target.handler:
            return target.handler
        elif target.name in self.handlers:
            # first try using target's name
            return self.handlers.get(target.name)
        elif target.type and (target.type in self.handlers):
            # first try using target's type (if any)
            return self.handlers.get(target.type)
        # else return default handler
        return self.default_handler

    def to_path(self, target, **kwargs):
        """return path from target"""
        path = self.converter.to_path(target, **kwargs)
        return os.path.join(self.root, path)

    def from_path(self, path):
        """return target from path"""
        relpath = os.path.relpath(path, start=self.root)
        return self.converter.from_path(relpath)

    def location(self, target):
        """alias of self.to_path"""
        return self.to_path(target)


def make_handlers(handlers):
    """helper function to create a handlers dictionary"""
    if handlers is None:
        handlers = {}

    _handlers = {}
    for name, item in handlers.items():
        if isinstance(item, FileHandler):
            # if obj already a FileHandler
            _handlers[name] = item

        else:
            # if obj already a dict with readers and writers
            _handlers[name] = file_handler(item)

    return _handlers


# shutil
def removedirs(path, root=""):
    """like os.removedirs, but stop at root"""
    if os.path.isfile(path):
        return
    elif root:
        path = os.path.relpath(path, start=root)
    split = os.path.normpath(path).split(os.path.sep)
    nsplit = len(split)
    for i in range(nsplit, 0, -1):
        subpath = os.path.join(root, os.path.sep.join(split[:i]))
        if not os.path.exists(subpath):
            # if no dir, continue
            continue
        try:
            os.rmdir(subpath)
        except OSError:
            # non-empty dir
            break
