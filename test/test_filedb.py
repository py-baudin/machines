# -*- coding: utf-8 -*-
""" test machines.filedb """

import os
import datetime
import shutil
import json
import pytest
from machines.target import Target, Index, Branch
from machines import filedb
from machines.targetpath import TargetToPathDedicated


def test_removedirs(tmpdir):

    root = tmpdir.mkdir("root")

    # make directory tree
    os.makedirs(os.path.join(root, "a", "b", "c"))
    filedb.removedirs(os.path.join(root, "a", "b", "c"), root=os.path.join(root, "a"))
    assert not os.path.exists(os.path.join(root, "a", "b"))
    assert os.path.exists(os.path.join(root, "a"))

    os.makedirs(os.path.join(root, "a", "b", "c1"))
    os.makedirs(os.path.join(root, "a", "b", "c2"))

    filedb.removedirs(os.path.join(root, "a", "b", "c1"), root=os.path.join(root))
    assert os.path.exists(os.path.join(root, "a", "b", "c2"))

    filedb.removedirs(os.path.join(root, "a", "b", "c2"), root=os.path.join(root))
    assert not os.path.exists(os.path.join(root, "a"))
    assert os.path.exists(root)


def test_filedb(tmpdir):
    """test FileMap class"""

    root = tmpdir.join("root")
    db = filedb.FileDB(root)

    # eq
    assert db == filedb.FileDB(root)

    target = Target("name")
    assert not target in db
    assert not os.path.isdir(root)

    db[target] = {"foo": "bar"}
    assert target in db

    # check path
    assert "name" in os.listdir(root.join("_"))

    # read target
    data = db[target]
    assert data == {"foo": "bar"}

    # check iterator
    assert list(db) == [target]

    # overwrite target
    db[target] = {"foo": "baz"}
    data = db[target]
    assert data == {"foo": "baz"}

    # delete target
    del db[target]
    assert not target in db
    assert not os.path.isdir(root)  # tempdir was removed

    # add more targets and check iterator
    db[Target("name1", "id1")] = "data"
    db[Target("name2", None, "branch2")] = "data"
    db[Target("name3", "id3", "branch3")] = "data"
    db[Target("name4", ("id41", "id42"), ("branch41", "branch42"))] = "data"

    assert set(db) == {
        Target("name1", "id1"),
        Target("name2", None, "branch2"),
        Target("name3", "id3", "branch3"),
        Target("name4", ("id41", "id42"), ("branch41", "branch42")),
    }
    # check pathes
    assert root.join("id1", "name1").exists()
    assert root.join("_", "name2~branch2").exists()
    assert root.join("id3", "name3~branch3").exists()
    assert root.join("id41.id42", "name4~branch41.branch42").exists()

    # wrong keys
    with pytest.raises(KeyError):
        db[Target("unknown_target")]

    class WrongType:
        attachment = {}

    with pytest.raises(TypeError):
        db[WrongType()]


def test_filedb_dedicated(tmpdir):
    """test FileMap class with dedicated option"""

    root = tmpdir.join("root")
    db = filedb.FileDB(root, converter=TargetToPathDedicated("A"))

    target = Target("A", "id1")
    db[target] = "foobar"
    assert root.join("id1").exists()

    target = Target("A", "id1", "branch1")
    db[target] = "foobar"
    assert root.join("id1~branch1").exists()


#
# def test_filedb_with_backup(tmpdir):
#     """ test FileMapWithBackup class"""
#
#     root = tmpdir.mkdir("root")
#     db = filedb.FileDBwithBackup(root)
#
#     target1 = Target("A", "id1")
#     db[target1] = "foobar"
#     assert root.join("id1", "A_v1").exists()
#
#     # same target
#     target2 = Target("A", "id1")
#     db[target2] = "foobaz"
#     assert root.join("id1", "A_v2").exists()
#
#     assert db[target1] == "foobar"  # previous version still exists
#     assert db[target2] == "foobaz"
#     assert db[Target("A", "id1")] == "foobaz"  # last vesion
#
#     # specific version
#     assert db[Target("A", "id1", attach={"version": 1})] == "foobar"
#
#     # del
#     with pytest.raises(NotImplementedError):
#         del db[target2]
#
#     # overwrite
#     with pytest.raises(NotImplementedError):
#         db[target1] = "overwrite"
#
#     # contains
#     assert target1 in db
#     assert Target("A", "id1") in db
#
#     # add a different target 3 times
#     db[Target("B")] = "foobar1"
#     db[Target("B")] = "foobar2"
#     db[Target("B")] = "foobaz"
#
#     assert root.join("_", "B_v1").exists()
#     assert root.join("_", "B_v2").exists()
#     assert root.join("_", "B_v3").exists()
#
#     # list
#     targets = list(db)
#     assert len(targets) == 2
#     assert Target("A", "id1") in targets  # only last version
#     assert Target("B") in targets  # only last version
#     assert all(db[target] == "foobaz" for target in targets)
