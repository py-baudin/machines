# -*- coding: utf-8 -*-
""" test machines.handlers """
import os
import pytest

from machines import filedb
from machines.target import Target
from machines.handlers import FileHandler, file_handler, InvalidFileHandler
from machines import handlers


def test_file_handler_class(tmpdir):
    """test file handler class"""

    # with pytest.raises(InvalidFileHandler):
    #     FileHandler()

    class MyHandler(FileHandler):
        """basic handler with user-set filename"""

        def __init__(self, filename):
            """define filename"""
            self.filename = filename

        def _save(self, dirname, obj):
            with open(os.path.join(dirname, self.filename), "w") as f:
                f.write(obj)

        def _load(self, dirname):
            with open(os.path.join(dirname, self.filename), "r") as f:
                return f.read()

    # setup handlers
    handlers = {"A": MyHandler("A.data"), "typeA": MyHandler("typeA.data")}

    # setup filedb
    tempdir = os.path.join(tmpdir, "root")
    db = filedb.FileDB(tempdir, handlers=handlers)

    # get default handler
    default_handler = db.default_handler
    assert not default_handler in handlers.values()

    # use target name
    target1 = Target("A", 1)
    db[target1] = "foobar"
    path1 = db.to_path(target1)
    assert "A.data" in os.listdir(path1)

    # use default handler
    target2 = Target("B", 2)
    db[target2] = "foobar"
    path2 = db.to_path(target2)
    assert default_handler.filename in os.listdir(path2)

    # use target type
    target3 = Target("C", 3, type="typeA")
    db[target3] = "foobar"
    path3 = db.to_path(target3)
    assert "typeA.data" in os.listdir(path3)

    # use target name (ignore type)
    target4 = Target("A", 4, type="typeA")
    db[target4] = "foobar"
    path4 = db.to_path(target4)
    assert "A.data" in os.listdir(path4)


def test_file_handler():
    """test file_handler function"""
    target = Target("A")
    handler = file_handler(save=lambda path, data: None)
    assert isinstance(handler, FileHandler)
    assert handler.save(target, "dirname", "data") is None
    with pytest.raises(NotImplementedError):
        handler.load(target, "dirname")

    handler = file_handler(load=lambda path: None)
    assert handler.load(target, "dirname") is None
    with pytest.raises(NotImplementedError):
        handler.save(target, "dirname", "data")

    # with pytest.raises(InvalidFileHandler):
    #     file_handler()

    # multi handler
    h1 = {"load": lambda path: {"h1": path}, "save": lambda path, data: data.pop("h1")}
    h2 = {"load": lambda path: {"h2": path}, "save": lambda path, data: data.pop("h2")}
    handler = file_handler([h1, h2])

    data = handler.load(target, "path")
    assert data == {"h1": "path", "h2": "path"}

    handler.save(target, "path", data)
    assert data == {}

    # using dictionary
    memory = {}
    handler = file_handler(
        save={"A1": lambda path, data: memory.setdefault("A1", data)},
        load={"A1": lambda path: memory.get("A1"), "A2": lambda path: memory.get("A2")},
    )

    data = {"A1": "foobar", "A2": "foobaz"}
    handler.save(target, "path", data)
    assert memory["A1"] == "foobar"  # only A1 was saved
    data2 = handler.load(target, "path")
    # only A1 was loaded
    assert data2 == {"A1": "foobar", "A2": None}


def test_decorators():
    memory = {}

    @handlers.pass_target
    def saver(target, dirname, data):
        memory[dirname] = {"target": target, "data": data}

    @handlers.pass_target
    def loader(target, dirname):
        data = memory[dirname]
        assert data["target"] == target
        return data["data"]

    handler = file_handler(save=saver, load=loader)

    # save
    handler.save(Target("A"), "path", "foobar")
    assert memory["path"] == {"target": Target("A"), "data": "foobar"}

    # load
    assert handler.load(Target("A"), "path") == "foobar"
    with pytest.raises(AssertionError):
        handler.load(Target("B"), "path")
