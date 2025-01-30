# -*- coding: utf-8 -*-
""" unittest storages.py """

import os
import pathlib
import shutil
import pytest
import threading
import time
from collections import namedtuple
from machines.handlers import FileHandler
from machines.targetpath import TargetToPathExpr
from machines.storages import (
    TargetStorage,
    FileStorage,
    Target,
    TargetAlreadyExists,
    TargetDoesNotExist,
    TargetIsLocked,
    Status,
)


def _check_storage(storage):
    """test storage object"""

    target = Target("name", "id", "branch")

    # test exist
    assert not storage.exists(target)

    # test write
    storage.write(target, "data")
    assert storage.exists(target)

    # test no overwrite
    with pytest.raises(TargetAlreadyExists):
        storage.write(target, "data 2")

    # test not exist
    with pytest.raises(TargetDoesNotExist):
        storage.read(Target("name", "unknown", "br"))

    # test read
    data = storage.read(target)
    assert data == "data"

    # test location
    location = storage.location(target)
    if location != "memory":
        assert os.path.isdir(location)

    # test copy
    target2 = Target("name", "id2", "br")
    storage.copy(target, target2)
    assert storage.read(target2) == "data"

    # test list
    assert set(storage.list()) == {target, target2}

    # test mode="test" (no overwrite)
    storage.write(target2, "tested", mode="test")
    assert storage.read(target2) == "data"

    # test mode="overwrite
    storage.write(target2, "overwritten", mode="overwrite")
    assert storage.read(target2) == "overwritten"

    # test mode="upgrade"
    storage.write(target2, "upgraded", mode="upgrade")
    assert storage.read(target2) == "upgraded"

    # test clear
    storage.clear()
    assert not storage.exists(target)
    assert not storage.exists(target2)

    # check cleanup
    Task = namedtuple("Task", ["inputs", "status", "aggregate"])
    targetA = Target("name", "A")
    targetB = Target("name", "B")
    summary = [
        Task(inputs=[targetA], status=Status.SUCCESS, aggregate=False),
        Task(inputs=[targetB], status=Status.ERROR, aggregate=False),
    ]
    storage.write(targetA, "data")
    storage.write(targetB, "data")

    # if persistent
    storage.temporary = False
    storage.cleanup(summary)
    assert storage.exists(targetA)

    # if temporary
    storage.temporary = True
    storage.cleanup(summary)
    assert storage.exists(targetB)
    assert not storage.exists(targetA)

    storage.clear()


def test_target_storage_class():
    """test basic storage class"""
    storage = TargetStorage()
    _check_storage(storage)


def test_file_storage_class(tmpdir):
    """test file storage class"""

    tmpdir = pathlib.Path(tmpdir)

    # basic file storage
    storage = FileStorage(tmpdir / "basic")
    _check_storage(storage)

    # add a file handler
    class CustomHandler(FileHandler):
        def _path(self, dirname):
            return os.path.join(dirname, "foobar.txt")

        def _save(self, dirname, data):
            with open(self._path(dirname), "w") as f:
                f.write(data)

        def _load(self, dirname):
            with open(self._path(dirname), "r") as f:
                data = f.read()
            return data

    handler = CustomHandler()

    # with handler
    storage = FileStorage(tmpdir / "handler", handlers={"name": handler})
    _check_storage(storage)

    # with handler (2)
    storage = FileStorage(
        tmpdir / "handler2",
        handlers={"name": {"save": handler._save, "load": handler._load}},
    )
    _check_storage(storage)

    # dedicated storage
    converter = TargetToPathExpr(name="name")
    storage = FileStorage(tmpdir / "dedicated", converter=converter)
    _check_storage(storage)

    # # with version (int)
    # storage = FileStorage(tmpdir / "version", version="int")
    # _check_storage(storage)
    #
    # # test version stuff
    # storage.write(Target("A"), "data1")
    # storage.write(Target("A"), "data2", mode="upgrade")
    # assert len(storage.list()) == 2
    # storage.write(Target("A"), "data2", mode="upgrade")
    # assert len(storage.list()) == 2
    # assert storage.read(Target("A", version=1)) == "data1"
    # assert storage.read(Target("A", version=2)) == "data2"
    #
    # # with version (date)
    # storage = FileStorage(tmpdir / "version_date", version="date")
    # _check_storage(storage)
    #
    # # test version stuff
    # storage.write(Target("A"), "data1")
    # storage.write(Target("A"), "data2", mode="upgrade")
    # assert len(storage.list()) == 2
    # storage.write(Target("A"), "data2", mode="upgrade")
    # assert len(storage.list()) == 2


def test_callbacks():
    """test on_read, on_write, on_del"""

    def on_read(target):
        """check something before reading"""
        target.attach(read=True, overwrite=True)

    def on_write(target, value):
        """save some info in the db"""
        target.attach(written=True)

    def on_del(target):
        """callback on delete"""
        target.attach(deleted=True)

    def on_test(target, is_same):
        """callback on test"""
        target.attach(is_same=is_same)

    # create storage with callbacks
    dummydb = {}
    storage = TargetStorage(
        memory=dummydb,
        on_read=on_read,
        on_write=on_write,
        on_del=on_del,
        on_test=on_test,
    )
    target = Target("name")

    # write target
    storage.write(target, {"some": "data"})

    # check db
    assert target in dummydb
    assert target.attachment["written"]

    # test read
    assert storage.read(target) == {"some": "data"}
    assert target.attachment["read"]

    # test data
    storage.write(target, {"some": "other data"}, mode="test")
    assert not target.attachment["is_same"]
    assert storage.read(target) == {"some": "data"}

    # upgrade data
    target = Target("name")
    storage.write(target, {"some": "other data"}, mode="upgrade")
    assert not target.attachment["is_same"]
    assert storage.read(target) == {"some": "other data"}

    # remove value
    storage.remove(target)
    assert target.attachment["deleted"]
    assert not target in dummydb


def test_comparators():
    """test target comparator"""

    comparators = {"B": lambda x, y: x[0] == y[0]}

    memory = {}
    storage = TargetStorage(memory=memory, comparators=comparators)
    storage.write(Target("A"), ("foo", "bar"))
    storage.write(Target("A"), ("foo", "baz"), mode="upgrade")
    assert storage.read(Target("A")) == ("foo", "baz")  # was upgraded

    # with custom comparator
    storage.write(Target("B"), ("foo", "bar"))
    storage.write(Target("B"), ("foo", "baz"), mode="upgrade")
    assert storage.read(Target("B")) == ("foo", "bar")  # was no upgraded

    storage.write(Target("B"), ("foo", "baz"), mode="overwrite")
    assert storage.read(Target("B")) == ("foo", "baz")  # was upgraded


def test_storage_lock():
    storage = TargetStorage(target_lock=["A"])
    storage.write(Target("B"), "data1")
    storage.write(Target("B"), "data2", mode="overwrite")
    storage.write(Target("B"), "data2", mode="upgrade")
    storage.write(Target("B"), "data2", mode="test")

    storage.write(Target("A"), "data1")
    with pytest.raises(TargetIsLocked):
        storage.write(Target("A"), "data2", mode="overwrite")
    with pytest.raises(TargetIsLocked):
        storage.write(Target("A"), "data2", mode="upgrade")
    with pytest.raises(TargetIsLocked):
        storage.write(Target("A"), "data2", mode="test")


def test_storage_test(tmpdir):
    storage = TargetStorage()
    assert storage.check(Target("any", "any")) is None
    assert storage.check("Something else") is None

    converter = TargetToPathExpr(name="name", values={"id": ["foo", "bar"]})
    storage = FileStorage(tmpdir / "storage1", converter=converter)
    assert storage.check(Target("name", "foo")) is None
    with pytest.raises(ValueError):
        storage.check(Target("name", "wrong"))
    with pytest.raises(ValueError):
        storage.check(Target("wrong", "foo"))


def test_thread_safety(tmpdir):
    """test thread-safety of Storage Class"""

    root = tmpdir.mkdir("root")

    storage = FileStorage(root)
    target = Target("name")
    storage.write(target, "foobar")

    def update(i):
        try:
            storage.remove(target)
        except TargetDoesNotExist:
            pass

        try:
            storage.write(target, "blah%d" % i * 100)
        except TargetAlreadyExists:
            pass

    threads = []
    for i in range(100):
        thread = threading.Thread(target=update, args=[i])
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    assert storage.read(target) != "foobar"
