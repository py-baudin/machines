# -*- coding: utf-8 -*-
""" test Session class """


import os
import json
from machines.target import Target
from machines.storages import MemoryStorage, FileStorage
from machines.handlers import Serializer
from machines.decorators import machine
from machines.factory import factory, hold, MAIN_STORAGE, TEMP_STORAGE
from machines.toolbox import Toolbox
from machines.session import Session, basic_session, setup_storages
from machines.parameters import VariableIO


def test_session():
    """test Session class"""

    # create program
    @machine(output="A", p1=(int, 1))
    def MachineA(p1):
        return "foo" * p1

    @machine(inputs="A", output="B", p2=(["bar", "baz"], "bar"))
    def MachineB(A, p2):
        return A + p2

    # make toolbox
    toolbox = Toolbox("session", description="does something")

    # make process from list
    toolbox.add_program("prog1", [MachineA, MachineB])

    # init session
    storage = MemoryStorage()
    session = Session(toolbox, {MAIN_STORAGE: storage})

    # run session
    tasks = session.run(
        "prog1", indices=1, parameters={"p1": 2, "p2": "bar"}, hold=True
    )
    assert storage.read(Target("B", 1)) == "foofoobar"

    # make process from dict
    @machine(output="C", p3=(str, "default"))
    def MachineC(p3):
        return p3

    # add new process
    toolbox.add_program("prog2", {"a": [MachineA, MachineB], "b": MachineC})

    # init session
    storage = MemoryStorage()
    session = Session(toolbox, {MAIN_STORAGE: storage})

    session.run(
        "prog2", indices=1, parameters={"choice": "a", "p1": 2, "p2": "baz"}, hold=True
    )
    assert storage.read(Target("B", "1")) == "foofoobaz"

    session.run("prog2", indices=2, parameters={"choice": "b", "p3": "test"}, hold=True)
    assert storage.read(Target("C", 2)) == "test"


def test_basic_session(tmpdir):
    """test basic session function"""

    @machine(output="A", p1=(str, "foo"), p2=int)
    def MachineA(p1, p2):
        return p1 * p2

    @machine(inputs="A", output="B", p3=["bar", "baz"])
    def MachineB(A, p3):
        return A + p3

    # make toolbox
    toolbox = Toolbox("basic-session")
    toolbox.add_program("prog1", [MachineA, MachineB])

    # make session
    workdir = tmpdir.join("work")
    tempdir = tmpdir.join("temp")
    main_storage = FileStorage(workdir)
    temp_storage = FileStorage(tempdir, temporary=True)
    session = basic_session(toolbox, main_storage, temp=temp_storage)

    # run prog1
    tasks = session.run(
        "prog1", indices=[1, 2], parameters={"p2": 2, "p3": "bar"}, hold=True
    )

    # check storages
    assert {"1", "2"} <= set(os.listdir(workdir))
    assert ["B"] == os.listdir(os.path.join(workdir, "1"))
    assert ["B"] == os.listdir(os.path.join(workdir, "2"))

    # won't run
    session.run("prog1", indices=2, parameters={"p2": 1, "p3": "baz"}, hold=True)
    assert main_storage.read(Target("B", 1)) == "foofoobar"
    assert main_storage.read(Target("B", 2)) == "foofoobar"

    # will run
    session.run(
        "prog1",
        indices=2,
        parameters={"p2": 3, "p3": "baz"},
        hold=True,
        mode="overwrite",
    )
    assert main_storage.read(Target("B", 1)) == "foofoobar"
    assert main_storage.read(Target("B", 2)) == "foofoofoobaz"

    #
    # test tempdir

    @machine(inputs="A", output="B", p3=["bar", "baz"])
    def MachineB_err(A, p3, identifier_B):
        if identifier_B.index == "3":
            1 / 0
        return A + p3

    # add process
    toolbox.add_program("prog1_err", [MachineA, MachineB_err])

    # run program with errors
    session.run(
        "prog1_err", indices=[3, 4], parameters={"p2": 2, "p3": "bar"}, hold=True
    )

    # check storage in tempdir
    assert os.listdir(tempdir)
    assert temp_storage.read(Target("A", 3)) == "foofoo"  # only A was run
    assert not main_storage.exists(Target("B", 3)) == "foofoo"  # only A was run
    assert main_storage.read(Target("B", 4)) == "foofoobar"  # A and B were run

    # kill current session and restart
    session.close()
    session = basic_session(toolbox, main_storage, temp=temp_storage)

    # resume process with a different parameter value for param2
    session.run("prog1", indices=[3, 4], parameters={"p2": 2, "p3": "baz"}, hold=True)

    # check storage
    assert main_storage.read(Target("B", 3)) == "foofoobaz"  # A not overwritten
    assert main_storage.read(Target("B", 4)) == "foofoobar"  # B not overwritten

    # tempdir was removed
    assert not os.path.isdir(tempdir)

    # kill current session
    session.close()

    #
    # test targetdirs

    @machine(inputs=["A", "B"], output="C")
    def MachineC(A, B):
        return A + B

    # add new program
    toolbox.add_program("prog2", [MachineC])

    # make target storage
    dirA = tmpdir.join("Adir")
    storage_A = FileStorage(dirA, at_root="A")
    session = basic_session(
        toolbox, main_storage, temp=temp_storage, dedicated={"A": storage_A}
    )

    session.run("prog1", indices=5, parameters={"p2": 1, "p3": "bar"})
    session.run("prog1", indices=6, parameters={"p2": 2, "p3": "baz"})
    session.run("prog2", indices=[5, 6], hold=True)

    # check output "A" is in separate directory
    assert os.listdir(dirA)
    assert storage_A.read(Target("A", 5)) == "foo"
    assert storage_A.read(Target("A", 6)) == "foofoo"
    assert main_storage.read(Target("C", 5)) == "foofoobar"
    assert main_storage.read(Target("C", 6)) == "foofoofoofoobaz"

    # test session info
    info = session.info
    assert "toolbox" in info
    assert "factory" in info
    assert "storages" in info


def test_session_replay():
    """test replay"""

    # create program
    @machine(output="A", p1=(int, 1))
    def MachineA(p1):
        return "foo" * p1

    @machine(inputs="A", output="B", p2=["bar", "baz"])
    def MachineB(A, p2):
        return A + p2

    # make toolbox
    toolbox = Toolbox("session", description="does something")
    toolbox.add_program("prog", [MachineA, MachineB])

    # init session
    storage = MemoryStorage()
    session = Session(toolbox, {MAIN_STORAGE: storage})

    # run session
    history = {}
    tasks = session.run(
        "prog",
        indices="id1",
        parameters={"p1": 2, "p2": "baz"},
        hold=True,
        history=history,
    )
    assert storage.read(Target("B", "id1")) == "foofoobaz"
    assert "id1#B~" in history

    # replay
    storage2 = MemoryStorage()
    session2 = Session(toolbox, {MAIN_STORAGE: storage2})

    session2.replay(history["id1#B~"], hold=True)
    assert storage2.read(Target("B", "id1")) == "foofoobaz"


def test_session_autorun():
    """test Session.autorun"""

    # create program
    @machine(output="A", p1=(int, 1))
    def MachineA(p1):
        return "foo" * p1

    @machine(output="B", p2=["bar", "baz"])
    def MachineB(p2):
        return p2

    @machine(inputs=["A", "B"], output="C")
    def MachineC(A, B):
        return A + B

    # make toolbox
    toolbox = Toolbox("session", description="does something")

    # make process from list
    toolbox.add_program("progA", MachineA)
    toolbox.add_program("progB", MachineB)
    toolbox.add_program("progC", MachineC)

    # init session
    storage = MemoryStorage()
    session = Session(toolbox, {MAIN_STORAGE: storage})

    # run session
    tasks = session.autorun(
        "progC",
        indices="id1",
        parameters={"p1": 2, "p2": "baz"},
        hold=True,
        show_all=True,
    )
    assert len(tasks) == 3
    assert all(task.status.name == "SUCCESS" for task in tasks)
    assert storage.read(Target("C", "id1")) == "foofoobaz"

    # session.monitor
    summary = session.monitor()
    assert {task.output.name for task in summary} == {"C"}
    assert {task.status.name for task in summary} == {"SUCCESS"}

    # same with metamachines
    machineD = machine(MachineC, output="D")
    toolbox.add_program("progD", [MachineB, machineD])
    tasks = session.autorun(
        "progD",
        indices="id2",
        parameters={"p1": 3, "p2": "bar"},
        hold=True,
        show_all=True,
    )

    assert len(tasks) == 3
    assert all(task.status.name == "SUCCESS" for task in tasks)
    assert storage.read(Target("D", "id2")) == "foofoofoobar"

    # session.monitor
    summary = session.monitor()
    assert {task.output.name for task in summary} == {"C", "D"}
    # assert {task.meta["program"] for task in summary} == {"progC", "progD"}
    assert {task.identifier for task in summary} == {("id1", None), ("id2", None)}


#
# def test_session_export(tmpdir):
#
#     # create program
#     @machine(output="A", p1=int)
#     def MachineA(p1):
#         return "foo" * p1
#
#     # make toolbox
#     toolbox = Toolbox("session", description="does something")
#
#     # make process from list
#     toolbox.add_program("progA", MachineA)
#
#     # init session
#     workdir = tmpdir.mkdir("work")
#     handlers = {"A": Serializer(json, ext=".json", binary=False)}
#     storage = FileStorage(workdir, handlers=handlers)
#
#     record_file = tmpdir.join("records.json")
#     records = MiniDB(record_file)
#     session = Session(toolbox, {"DEFAULT": storage}, records=records)
#     assert session.records is records
#
#     # run session
#     tasks = session.run("progA", indices=["id1", "id2"], parameters={"p1": 2}, hold=True)
#     assert all(session.factory.read(task["output"]) == "foofoo" for task in tasks)
#
#     exportdir = tmpdir.mkdir("export")
#     to_export = [Target("A", "id1"), Target("A", "id2")]
#
#     summary = session.export(to_export, exportdir)
#     assert len(summary) == 2
#     assert all(summary[task]["status"].name == "SUCCESS" for task in summary)
#     assert len(records) == 2
#
#     # modify data
#     for task in summary:
#         path = exportdir.join(summary[task]["path"])
#         assert os.path.isfile(os.path.join(path, "data.json"))
#         with open(os.path.join(path, "data.json"), "w") as f:
#             json.dump("new", f)
#
#     summary = session.import_(exportdir, overwrite=True)
#     assert len(summary) == 2
#     assert all(summary[task]["status"].name == "SUCCESS" for task in summary)
#     assert len(records) == 0


def test_setup_storage(tmpdir):
    """test storage setup"""

    # build toolbox
    @machine(output="A")
    def MachineA():
        pass

    @machine(inputs="TBD", output="B", TBD=VariableIO(type="A"))
    def MachineB(TBD):
        pass

    @machine(inputs="b::B", output="C")
    def MachineC(b):
        pass

    toolbox = Toolbox("my-toolbox")
    toolbox.add_program("progA", MachineA)
    toolbox.add_program("progB", MachineB)
    toolbox.add_program("progC", MachineC)

    # make storages
    workdir = tmpdir / "work1"
    storages = setup_storages(toolbox, workdir)
    assert set(storages) == {MAIN_STORAGE}

    # make session
    session = Session(toolbox, storages)
    session.run("progA", indices="id1")
    session.run("progB", indices="id1", parameters={"TBD": "A"})
    session.run("progC", indices="id1", hold=True)
    summary = set(session.list())
    assert summary == {Target("A", "id1"), Target("B", "id1"), Target("C", "id1")}

    # use targetdirs
    workdir = tmpdir / "work2"
    reserved = tmpdir / "reserved2"
    targetdirs = [{"name": "B", "path": reserved}]
    storages = setup_storages(toolbox, workdir, targetdirs=targetdirs)
    assert set(storages) == {MAIN_STORAGE, "B"}

    # make session
    session = Session(toolbox, storages)
    session.run("progA", indices="id2")
    session.run("progB", indices="id2", parameters={"TBD": "A"})
    session.run("progC", indices="id2", hold=True)
    summary = set(session.list())
    assert summary == {Target("A", "id2"), Target("B", "id2"), Target("C", "id2")}

    # use targetdirs with VariableIO
    workdir = tmpdir / "work3"
    reserved = tmpdir / "reserved3"
    targetdirs = [
        {"name": "A", "path": reserved},
        {"name": "reserved", "path": reserved},
    ]
    storages = setup_storages(toolbox, workdir, targetdirs=targetdirs)
    assert set(storages) == {MAIN_STORAGE, "reserved", "A"}
    assert storages["A"].memory == storages["reserved"].memory

    # make session
    session = Session(toolbox, storages)
    session.run("progA", indices="id3")
    session.run("progB", indices="id3", parameters={"TBD": "reserved"})
    session.run("progC", indices="id3", hold=True)
    summary = set(session.list())

    # note: id3#A and id3#reserved are the same, but there are two targetdirs
    assert summary == {
        Target("A", "id3"),
        Target("reserved", "id3"),
        Target("B", "id3"),
        Target("C", "id3"),
    }
