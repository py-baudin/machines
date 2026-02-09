# -*- coding: utf-8 -*-
""" file handlers for targets """
import os
import pickle
import json


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
    

import abc
class BaseFileHandler(abc.ABC):

    @abc.abstractmethod
    def load(self, target, dirname):
        pass

    @abc.abstractmethod
    def save(self, target, dirname, data):
        pass



class FileHandler(BaseFileHandler):
    """Base class for handling target data"""

    _save = None
    _load = None

    def __init__(self, save=None, load=None):
        self.set(save=save, load=load)
        # if not self._save and not self._load:
        #     raise InvalidFileHandler("At least one of 'save' and 'load' must be set")
        
    def set(self, save=None, load=None):
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


#
import pathlib
import collections

class DeferredMapping(collections.abc.Mapping):
    """ Dictionary with deferred data loading """

    class DeferredItem:
        def __init__(self, func, args=[], kwargs={}):
            self.func = func
            self.args = args
            self.kwargs = kwargs
        def __call__(self):
            return self.func(*self.args, **self.kwargs)
        def __repr__(self):
            return f'Deferred({self.func.__name__}, *{self.args}, **{self.kwargs})'
        
    def defer(self, key, func, args=[], kwargs={}):
        item = self.DeferredItem(func, args=args, kwargs=kwargs)
        self._memory[key] = item

    def set(self, key, value):
        self._memory[key] = value

    def __init__(self, *args, **kwargs):
        self._memory = dict(*args, **kwargs)

    def __len__(self):
        return len(self._memory)
    
    def __iter__(self):
        return iter(self._memory)
    
    def __contains__(self, key):
        return key in self._memory
    
    def __getitem__(self, key):
        if isinstance(self._memory[key], self.DeferredItem):
            # load data and store in memorys
            self._memory[key] = self._memory[key]()
        return self._memory[key]
    
    def __repr__(self):
        return f'Deferred({repr(self._memory)})'

    # mixin:
    # __contains__, keys, items, values, get, __eq__, and __ne__



class MultiHandler(BaseFileHandler):
    def __init__(self):
        self._savers = {}
        self._loaders = {}
    
    def saver(self, typename, ext='', pass_target=False, kwargs={}):
        """ define saver for typename (decorator)"""
        def wrapper(func):
            def wrapped(target, dirname, data):
                if not isinstance(data, collections.abc.Mapping):
                    raise ValueError(f'Expecting mapping, not {type(data)}')
                dirname = pathlib.Path(dirname)
                for name in data:
                    filename = dirname / (name + ext)
                    args = (filename, data[name])
                    if pass_target:
                        args = (target, *args)
                    func(*args, **kwargs)
            self._savers[typename] = wrapped
        return wrapper
    
    def loader(self, typename, ext=None, pattern='*', *, deferred=True, pass_target=False, kwargs={}):
        """ define loader  for typename (decorator)"""
        exts = []
        if ext:
            exts = ext if isinstance(ext, (list, tuple)) else [ext]

        def wrapper(func):
            def wrapped(target, dirname):
                dirname = pathlib.Path(dirname)
                data = DeferredMapping()
                for ext in exts:
                    for filename in dirname.glob(pattern):
                        name = filename.name.rsplit('.')[0]
                        if name in data:
                            continue
                        if not filename.name.endswith(ext):
                            continue
                        args = (filename,)
                        if pass_target:
                            args = (target, *args)
                        if deferred:
                            data.defer(name, func, args=args, kwargs=kwargs)
                        else:
                            data.set(name, func(*args, **kwargs))
                return data
            self._loaders[typename] = wrapped
        return wrapper
    
    def save(self, target, dirname, data):
        """ save data """
        if not isinstance(data, collections.abc.Mapping):
            raise ValueError(f'In {target}, `data` must be a mapping, not: {type(data)}')
        invalid = set(data) - set(self._savers)
        if invalid: 
            raise ValueError(f'In {target}, no handler found for data category(ies): {list(invalid)}')
        for key in data:
            saver = self._savers[key]
            saver(target, dirname, data[key])
        
    def load(self, target, dirname):
        """ load data """ 
        data = {}
        for key in self._loaders:
            loader = self._loaders[key]
            data[key] = loader(target, dirname)
        return data

            

    