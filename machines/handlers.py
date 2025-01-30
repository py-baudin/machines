# -*- coding: utf-8 -*-
""" file handlers for targets """
import os
import pickle
import json
import inspect


class InvalidFileHandler(Exception):
    pass


def pass_target(func):
    """(decorator) instruct FileHandler to pass the target to saver/loader"""
    if not hasattr(func, "__file_handler_meta__"):
        setattr(func, "__file_handler_meta__", {})
    func.__file_handler_meta__["pass_target"] = True
    return func


def file_handler(obj=None, save=None, load=None):
    """helper function to create FileHandler objects"""
    if isinstance(obj, FileHandler):
        # already a file handler
        return obj

    elif isinstance(obj, dict):
        # single dict
        return FileHandler(save=obj.get("save"), load=obj.get("load"))

    elif isinstance(obj, list):
        # chain of handlers
        handlers = [file_handler(item) for item in obj]
        return ChainedHandler(handlers)

    elif obj:
        raise InvalidFileHandler(f"Cannot make handler with object: {obj}")

    else:
        return FileHandler(save=save, load=load)


class FileHandler:
    """Base class for handling target data"""

    _save = None
    _load = None

    def __init__(self, save=None, load=None):
        if save:
            if isinstance(save, dict):
                self._save = keyword_saver(save)
            elif callable(save):
                self._save = save
            else:
                raise TypeError(f"Saver must be callable, not {save}")
        if load:
            if isinstance(load, dict):
                self._load = keyword_loader(load)
            elif callable(load):
                self._load = load
            else:
                raise TypeError(f"Loader must be callable, not {load}")

        if not self._save and not self._load:
            raise InvalidFileHandler("At least one of 'save' and 'load' must be set")

    def load(self, target, dirname):
        if not self._load:
            raise NotImplementedError("No loader provided")
        args = (dirname,)
        handler_meta = getattr(self._load, "__file_handler_meta__", {})
        if handler_meta.get("pass_target") == True:
            args = (target,) + args
        return self._load(*args)

    def save(self, target, dirname, data):
        if not self._save:
            raise NotImplementedError("No saver provided")
        args = (dirname, data)
        handler_meta = getattr(self._save, "__file_handler_meta__", {})
        if handler_meta.get("pass_target") == True:
            args = (target,) + args
        return self._save(*args)

    def __repr__(self):
        name = type(self).__name__
        return f"{name}"


def keyword_saver(savers):
    """combine savers from dictionary"""
    if not isinstance(savers, dict):
        raise TypeError(f"Invalid dictionary: {savers}")
    if not all(callable(func) for func in savers.values()):
        raise TypeError(f"Savers must be callables: {savers}")

    def saver(dirname, data):
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict object, received: {type(data)}")
        for key in data:
            if not key in savers:
                continue
            # save data
            savers[key](dirname, data[key])

    return saver


def keyword_loader(loaders):
    """combine loaders from dictionary"""
    if not isinstance(loaders, dict):
        raise TypeError(f"Invalid dictionary: {loaders}")
    if not all(callable(func) for func in loaders.values()):
        raise TypeError(f"Loaders must be callables: {loaders}")

    def loader(dirname):
        data = {}
        for key in loaders:
            # load data
            data[key] = loaders[key](dirname)
        return data

    return loader


class ChainedHandler(FileHandler):
    """make file-handler from sequence of handlers"""

    def __init__(self, handlers):
        # chain of handlers
        if not all(isinstance(h, FileHandler) for h in handlers):
            raise InvalidFileHandler("All items must be FileHandlers instances")
        self.handlers = handlers

    def save(self, target, dirname, data):
        if not isinstance(data, dict):
            raise TypeError("Output data must be a dict when using chained handlers")
        for h in self.handlers:
            h.save(target, dirname, data)

    def load(self, target, dirname):
        data = {}
        for h in self.handlers:
            data.update(h.load(target, dirname))
        return data


class Serializer(FileHandler):
    """File handler using module.load & module.dump"""

    def __init__(self, module, ext, binary=False):
        self.module = module
        self.bin = "b" if binary else ""
        self.filename = "data" + ext

    def _load(self, dirname):
        """load object in directory"""
        with open(os.path.join(dirname, self.filename), "r" + self.bin) as f:
            return self.module.load(f)

    def _save(self, dirname, obj):
        """save object to directory"""
        with open(os.path.join(dirname, self.filename), "w" + self.bin) as f:
            self.module.dump(obj, f)

    def __repr__(self):
        return f"Serializer({self.module.__name__})"


# simple handler with pickle
pickle_handler = Serializer(pickle, ".pickle", binary=True)

# simple handler with json pickle
json_handler = Serializer(json, ".json", binary=False)
