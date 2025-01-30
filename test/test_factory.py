# -*- coding: utf-8 -*-
""" test factory.py """

import os
import pytest
import threading
import time
import tempfile
import array

from machines.common import Status
from machines.target import Target
from machines.decorators import machine
from machines.factory import (
    TaskQueue,
    factory,
    Factory,
    get_current_factory,
    get_factory,
    factory_exists,
)


def test_taskqueue_sorting():
    """test task sorting in queue"""

    queue = TaskQueue()

    noid = Target("noid")
    queue.put(noid)

    # tasks A
    A_id2 = Target("A", "id2")
    A_id1_br1 = Target("A", "id1", "br1")
    queue.put(A_id2)
    queue.put(A_id1_br1)

    # tasks B
    B_id1_br1 = Target("B", "id1", "br1")
    B_id1 = Target("B", "id1")
    queue.put(B_id1_br1)
    queue.put(B_id1)

    # tasks C
    C_id2_br2 = Target("C", "id2", "br2")
    C_id2_br1 = Target("C", "id2", "br1")
    C_id1 = Target("C", "id1")
    C_id1_br1 = Target("C", "id1", "br1")
    queue.put(C_id2_br2)
    queue.put(C_id2_br1)
    queue.put(C_id1)
    queue.put(C_id1_br1)

    tasks = list(queue)

    """ sorting:
       1. by index (None => last)
       2. branch (None => first)
    no sorting by name
    """
    assert tasks[0] == B_id1
    assert tasks[1] == C_id1
    assert tasks[2] == A_id1_br1
    assert tasks[3] == B_id1_br1
    assert tasks[4] == C_id1_br1
    assert tasks[5] == A_id2
    assert tasks[6] == C_id2_br1
    assert tasks[7] == C_id2_br2
    assert tasks[8] == noid


def test_taskqueue_threadsafe():
    """test sorted queue
    This is not a strong test
    """

    global n, maxitems
    maxitems = 0
    n = 0

    def assert_locked(tasks):
        global n, maxitems
        n += 1
        # count threads
        assert n == 1
        maxitems = max(maxitems, len(tasks))
        time.sleep(0.001)
        n -= 1

    queue = TaskQueue(lockcb=assert_locked)

    global id
    id = 0

    def put():
        global id
        queue.put(Target("A", id))
        id += 1

    def get():
        time.sleep(0.001)
        queue.get()

    threads = []
    for i in range(100):
        t = threading.Thread(target=put)
        threads.append(t)
        t = threading.Thread(target=get)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    assert n == 0
    assert maxitems > 1


def test_factory_manager():
    """test factory with TaskQueue"""

    @machine(output="A")
    def SomeMachine():
        time.sleep(0.001)
        return "foobar"

    lock = True

    @machine(output="A")
    def HoldMachine():
        nonlocal lock
        while lock:
            time.sleep(0.001)

    # test hold option
    with factory(hold=False) as fy:
        assert not Factory.hold_current
        task1 = HoldMachine.single(1)
        task2 = SomeMachine.single(2)
    assert task1.status == Status.RUNNING  # running
    assert task2.status == Status.NEW  # not done yet

    # hold factory
    lock = False
    fy.hold()
    assert task2.status == Status.SUCCESS

    # hold option in context manager
    with factory(hold=True):
        assert Factory.hold_current
        task1 = HoldMachine.single(1)
        task2 = SomeMachine.single(2)
    assert task1.status == Status.SUCCESS
    assert task2.status == Status.SUCCESS

    # test basic usage
    with factory(hold=True):
        tasks = SomeMachine(list(range(10)))
    assert len(tasks) == 10
    assert all(task.status == Status.SUCCESS for task in tasks)

    # context object
    with factory() as fac1:
        fac2 = get_current_factory()
        fac3 = get_factory(fac2.name)
        assert factory_exists(fac3.name)
        assert not factory_exists("dummy name")
        fac4 = Factory.current_factory
    assert fac1 is fac2 is fac3 is fac4
    assert fac1 is not Factory.current_factory  # out of context
    with pytest.raises(RuntimeError):
        get_current_factory()  # out of context

    # task list
    lock = True
    with factory() as fy:
        SomeMachine.single(1)
        HoldMachine.single(2)
        SomeMachine.single(3)
    tasks = fy.tasks

    def waitfor(task, status):
        while task.status.name != status:
            time.sleep(0.001)
        return True

    assert waitfor(tasks[0], "SUCCESS")
    assert waitfor(tasks[1], "RUNNING")
    assert waitfor(tasks[2], "NEW")  # factory is working the above

    lock = False
    assert waitfor(tasks[1], "SUCCESS")
    assert waitfor(tasks[2], "SUCCESS")

    # callback
    foobar = None

    def callback(summary):
        nonlocal foobar
        foobar = summary[-1].output_data

    with factory(hold=True, callback=callback):
        tasks = SomeMachine()
    assert foobar == "foobar"

    # default workdir
    with tempfile.TemporaryDirectory() as tmpdir:
        with factory(root=tmpdir) as fy:
            assert str(fy.main_storage.memory.root) == tmpdir


def test_dry_factory_class():
    """test DryFractory class"""

    @machine(output="A")
    def SomeMachine():
        return "Something"

    with factory(dry=True, hold=True):
        task = SomeMachine()[0]
    assert task.status == Status.NEW


def test_factory_stop():
    """test stopping factory"""

    @machine(output="A")
    def LongTask():
        time.sleep(0.1)

    with factory(hold=True) as fy:
        # run many tasks
        tasks = LongTask([str(i) for i in range(10)])
        fy.stop()

    # only one task is done
    assert tasks[0].status.name == "SUCCESS"
    assert all(task.status.name == "NEW" for task in tasks[1:])


# def test_factory_memory(tmpdir):
#     """ check memory handling """
#
#     @machine(output="A")
#     def Register(index_A):
#         """ load a heavy dataset """
#         print("Register", index_A)
#         arr = array.array("B", [1]*2**27)
#
#     @machine(inputs="A")
#     def Run(A, index_A):
#         """ Does something with the dataset """
#         print("Run", index_B)
#         b = A * "error"
#
#     with factory(hold=True, root=tmpdir):
#         Register(ids=["1", "2", "3"])
#         tasks = Run(ids=["1", "2", "3"])
#
#         1/0
