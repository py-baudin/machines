# -*- coding: utf-8 -*-
""" test machines.machine """

import time
import pytest

from machines.common import Status, RejectException
from machines.io import Output, Input
from machines.target import Target, Branch, Index
from machines.parameters import Parameter, VariableIO, Freeze
from machines.storages import MemoryStorage
from machines.factory import factory, hold, get_current_factory
from machines.machine import Machine
from machines.task import get_context
from machines.decorators import machine, metamachine
from machines import decorators


def test_machine_class():
    """test Machine class"""

    # dummy function
    def dummy(A, p1):
        pass

    # create machine
    machine1 = Machine(dummy)
    machine1.set_input("A", Input("A"))
    machine1.set_output("B", Input("B"))
    machine1.set_parameter("p1", Parameter(int))

    assert machine1.func is dummy
    assert machine1.name == "dummy"  # the function name
    assert machine1.inputs == {"A": [Input("A")]}
    assert machine1.outputs == {"B": [Output("B")]}
    assert machine1.output == [Output("B")]
    assert set(machine1.parameters) == {"p1"}
    assert isinstance(machine1.parameters["p1"], Parameter)

    # info
    assert machine1.info["name"] == "dummy"
    assert machine1.info["inputs"] == {"A": [{"dest": "A", "type": None}]}
    assert machine1.info["outputs"] == {"B": [{"dest": "B", "type": None}]}
    assert machine1.info["parameters"] == {"p1": machine1.parameters["p1"].info}

    # shorter equivalent
    machine1 = Machine(
        dummy, inputs=[Input("A")], output=Output("B"), p1=Parameter(int)
    )

    assert machine1.func is dummy
    assert machine1.name == "dummy"  # the function name
    assert machine1.inputs == {"A": [Input("A")]}
    assert machine1.outputs == {"B": [Output("B")]}
    assert machine1.output == [Output("B")]
    assert set(machine1.parameters) == {"p1"}

    # even shorted equivalent
    machine1 = Machine(dummy, inputs="A", output="B", p1=int)

    assert machine1.func is dummy
    assert machine1.name == "dummy"  # the function name
    assert machine1.inputs == {"A": [Input("A")]}
    assert machine1.outputs == {"B": [Output("B")]}
    assert machine1.output == [Output("B")]
    assert set(machine1.parameters) == {"p1"}

    # no inputs
    def dummy(p1):
        pass

    machine2 = Machine(dummy, output="B", p1=int)

    assert machine2.inputs == {}
    assert machine2.output == [Output("B")]

    # no output
    def dummy(A, p1):
        pass

    machine3 = Machine(dummy, inputs="A", p1=int)

    assert machine3.inputs == {"A": [Input("A")]}
    assert machine3.output is None

    # multiple inputs and parameters
    def dummy(A, B, p1, p2):
        pass

    machine4 = Machine(dummy, inputs=["A", "B"], p1=["a", "b"], p2=(str, "foobar"))

    assert machine4.inputs == {"A": [Input("A")], "B": [Input("B")]}
    assert set(machine4.parameters) == {"p1", "p2"}

    # alternative inputs
    machine5 = Machine(lambda A: None)
    machine5.set_input("A", Input("A1"))
    machine5.set_input("A", Input("A2"))
    machine5.set_output("B", Output("B1"))
    with pytest.raises(ValueError):
        # no alternative outputs
        machine5.set_output("B", Output("B2"))

    assert machine5.inputs == {"A": [Input("A1"), Input("A2")]}
    assert machine5.outputs == {"B": [Input("B1")]}

    # undefined inputs and copy
    machine6 = Machine(lambda A: None)
    machine6.set_input("A", Input(...))
    assert machine6.inputs == {"A": [Input(...)]}

    machine6_2 = machine6.copy()
    machine6_2.set_input("A", Input("A1"), replace=True)
    assert machine6_2.inputs == {"A": [Input("A1")]}

    # test argument validity

    with pytest.raises(ValueError):
        # missing inputs in signature
        Machine(lambda p1: None, inputs="A")

    with pytest.raises(ValueError):
        # missing input in signature
        Machine(lambda A, p: None, inputs="B", p1=int)

    with pytest.raises(ValueError):
        # missing parameter in signature
        Machine(lambda p1: None, p2=int)

    # this is valid
    Machine(lambda A, p: None, inputs="A", output="B")

    # this also: inputs instead of individual inputs
    Machine(lambda inputs: None, inputs=["A", "B"], output="C")

    # this also: group instead of individual inputs
    Machine(lambda gp, C: None, inputs=["A", "B", "C"], groups={"gp": ["A", "B"]})


def test_machine_ios():
    """test i/o mini language"""

    def dummy(A, B, p1, p2):
        pass

    # invalid io syntax
    with pytest.raises(ValueError):
        Machine(dummy, inputs="A A")

    with pytest.raises(ValueError):
        Machine(dummy, output="A & B")

    with pytest.raises(ValueError):
        Machine(dummy, output="A | B")

    # test inputs with types and dest
    def dummy(A, B):
        pass

    machine5 = Machine(dummy, inputs="A:Atype & B::Bdest", output="C:Ctype:Cdest")
    assert machine5.inputs["A"] == [Input("A")]
    assert machine5.inputs["A"][0].type == "Atype"
    assert machine5.inputs["B"] == [Input("Bdest")]
    assert machine5.outputs["C"] == [Output("Cdest")]
    assert machine5.output[0].type == "Ctype"

    # alt inputs
    machine5 = Machine(dummy, inputs="A::A1|A2 & B::B1", output="C::C1")
    assert machine5.inputs["A"] == [Input("A1"), Input("A2")]
    assert machine5.inputs["B"] == [Input("B1")]

    assert machine5.main_inputs == [Input("A1"), Input("B1")]
    assert machine5.main_outputs == [Output("C1")]
    assert machine5.flat_inputs == [Input("A1"), Input("A2"), Input("B1")]
    assert machine5.main_outputs == [Output("C1")]


def test_machine_helper():
    """test "machine" decorator"""

    # decorator
    @machine(inputs="A & B", output="C", p1=int)
    def machine1(A, B, p1):
        pass

    # checks
    assert machine1.name == "machine1"  # the function name
    assert machine1.inputs == {"A": [Input("A")], "B": [Input("B")]}
    assert machine1.outputs == {"C": [Output("C")]}
    assert machine1.parameters == {"p1": Parameter(int)}

    # same with decorators
    @machine
    @decorators.input("A")
    @decorators.input("B")
    @decorators.output("C")
    @decorators.parameter("p1", int)
    def machine1(A, B, p1):
        pass

    assert machine1.name == "machine1"  # the function name
    assert machine1.inputs == {"A": [Input("A")], "B": [Input("B")]}
    assert machine1.outputs == {"C": [Output("C")]}
    assert machine1.parameters == {"p1": Parameter(int)}

    # copy
    machine2 = machine(machine1, inputs="A & B::BB", p1=(int, 2))

    # checks
    assert machine2.name == "machine1"  # the function name
    assert machine2.inputs == {"A": [Input("A")], "B": [Input("BB")]}
    assert machine2.outputs == {"C": [Output("C")]}
    assert machine2.parameters == {"p1": Parameter(int, default=2)}


def test_basics():

    # simple machine
    @machine(inputs="A", output="B", p1=int)
    def machine1(A, p1=None):
        pass

    # running machine outside a factory
    with pytest.raises(RuntimeError):
        machine1(p1=2)

    # run machine without input
    with factory(hold=True):
        tasks = machine1(p1=2)

    # some checks on Task object
    assert len(tasks) == 1
    task = tasks[0]
    assert task.parameters == {"p1": 2}
    assert task.inputs == [Target("A")]
    assert all(not v for v in task.available_inputs.values())
    assert task.output == Target("B")
    assert task.status == Status.PENDING
    assert not task.ready()  # missing input
    assert not task.complete()  # did not run

    # run again with input
    with factory(hold=True) as fy:
        # add input
        fy.write(Target("A"), None)

        # run task
        task = machine1(p1=2)[0]

        assert task.ready()  # task can be run

    # when finished
    assert task.status == Status.SUCCESS
    assert task.complete()

    # check task properties
    assert task.index == Index(None)
    assert task.branch == Branch(None)
    assert task.identifier == (Index(None), Branch(None))

    # other argument settings
    @machine(output="B", p1=int)
    def machine2(p1, p2):
        return (p1, p2)

    with factory(hold=True):
        task1 = machine2(p1=1)[0]
        task2 = machine2(p1=1, p2="foobar")[0]
    assert task1.status == Status.ERROR  # missing argument p2
    assert task2.status == Status.SUCCESS
    assert task2.output_data == (1, "foobar")


def test_task_ios():
    """test various combinations of inputs/outputs"""

    # basic
    @machine(inputs=["A", "B"], output="C")
    def machine1(A, B):
        return A + B

    with factory(hold=True) as fy:
        fy.write(Target("A"), "foo")
        fy.write(Target("B"), "bar")
        task = machine1.single()
    assert task.status.name == "SUCCESS"
    assert task.output_data == "foobar"

    # alternative input definition
    @machine(inputs=["A", "B"], output="C")
    def machine3(inputs, A):
        assert "A" in inputs
        assert "B" in inputs
        assert inputs["A"] is A
        return A + inputs["B"]

    with factory(hold=True) as fy:
        # add targets A and B
        fy.write(Target("A"), "foo")
        fy.write(Target("B"), "bar")
        task = machine3.single()
    assert task.status.name == "SUCCESS"
    assert task.output_data == "foobar"

    # input groups
    @machine(inputs=["A", "B"], output="C", groups={"G": ["A", "B"]})
    def machine4(G):
        return G["A"] + G["B"]

    with factory(hold=True) as fy:
        fy.write(Target("A"), "foo")
        fy.write(Target("B"), "bar")
        task = machine4.single()
    assert task.status.name == "SUCCESS"
    assert task.output_data == "foobar"


def test_task_identifiers():
    """test ids and branches"""

    @machine()
    def machine5():  # no-output machine
        pass

    # no output
    with factory(hold=True):
        tasks0 = machine5(["id1", "id2"], "br1")
    assert tasks0[0].identifier == (Index(None), Branch(None))  # no index
    assert tasks0[1].identifier == (Index(None), Branch(None))  # no index

    # with output
    @machine(output="A")
    def dummy():
        pass

    # several ids
    with factory(hold=True):
        tasks1 = dummy(["id1", "id2"], "br1")
    assert tasks1[0].identifier == (Index("id1"), Branch("br1"))
    assert tasks1[1].identifier == (Index("id2"), Branch("br1"))

    # several branches
    with factory(hold=True):
        tasks2 = dummy("id1", ["br1", "br2"])
    assert tasks2[0].identifier == (Index("id1"), Branch("br1"))
    assert tasks2[1].identifier == (Index("id1"), Branch("br2"))

    # set output branch
    with factory(hold=True):
        tasks3 = dummy(["id1", "id2"], output_branches="br3")
    assert tasks3[0].identifier == (Index("id1"), Branch("br3"))
    assert tasks3[1].identifier == (Index("id2"), Branch("br3"))

    # add output branch
    with factory(hold=True):
        tasks4 = dummy(["id1", "id2"], "br1", output_branches="br3")
    assert tasks4[0].identifier == (Index("id1"), Branch("br1", "br3"))
    assert tasks4[1].identifier == (Index("id2"), Branch("br1", "br3"))

    # change output branch
    with factory(hold=True):
        tasks5 = dummy(["id1", "id2"], "br1", output_branches=["br4", "br4"])
    assert tasks5[0].identifier == (Index("id1"), Branch("br4"))
    assert tasks5[1].identifier == (Index("id2"), Branch("br4"))

    # change output ids
    with factory(hold=True):
        tasks6 = dummy(
            ["id1", "id2"], "br1", output_indices=["id3", "id4"]
        )  # change ids
    assert tasks6[0].identifier == (Index("id3"), Branch("br1"))
    assert tasks6[1].identifier == (Index("id4"), Branch("br1"))

    # bad: invalid number of output ids
    with factory(hold=True):
        with pytest.raises(ValueError):
            tasks = dummy(["id1", "id2"], "br1", output_indices="id3")

    # test input = output
    @machine(inputs="A", output="A")
    def dummy2(A, value=None):
        return value

    with factory(hold=True):
        task1 = dummy.single("id1")
        task2 = dummy2.single("id1", value="foobar", overwrite=True)
        task2 = dummy2.single("id1", value="foobaz", output_branches="branch1")

    assert task1.output_data == "foobar"
    assert task2.output_data == "foobaz"


def test_task_chain():
    """test chained tasks"""

    # no inputs
    @machine(output="A")
    def machine1():
        pass

    # both inputs and output
    @machine(inputs="A", output="B")
    def machine2(A):
        pass

    # no output
    @machine(inputs="B")
    def machine3(B):
        pass

    with factory():
        task2 = machine2.single()
        task3 = machine3.single()
        hold()
        assert not task2.ready()
        assert not task3.ready()

        task1 = machine1.single()
        assert task1.ready()  # always ready
        hold()

    # after everything is run
    assert task1.complete()
    assert task1.status == Status.SUCCESS
    assert task2.complete()
    assert task2.status == Status.SUCCESS
    assert not task3.complete()  # never complete
    assert task3.status == Status.SUCCESS  # but it was run anyway

    assert task2.ischild(task1)
    assert task3.ischild(task2)
    assert not task1.ischild(task2)
    assert not task3.ischild(task1)


def test_machine_copy():
    """test udpating machine"""

    # test meta inputs/outputs
    @machine(output="A")
    def machine1():
        return "foobar"

    @machine(inputs="B", output="C")
    def machine2(B):
        return B.upper()

    # make a copy with fixed input "B"
    machine3 = machine(machine2, inputs="B::A")

    with factory(hold=True):
        tasks1 = machine1()
        tasks2 = machine2(output_branches="pending")
        tasks3 = machine3(output_branches="copy")

    assert tasks2[0].status == Status.PENDING
    assert tasks3[0].status == Status.SUCCESS
    assert tasks3[0].output_data == "FOOBAR"

    # virual/concrete machine
    @machine
    @decorators.input("A", dest=Input(...))  # virtual input
    def virtual(A):
        return A

    # make concrete version
    concrete = machine(virtual, inputs="A")

    with factory(hold=True) as fy:
        fy.write(Target("A"), "foobar")
        with pytest.raises(RuntimeError):
            task1 = virtual.single()
        task2 = concrete.single()
    assert task2.status.name == "SUCCESS"


def test_requires_options():
    """test machine.requires=all/any option"""

    @machine(output="A")
    def machineA():
        return "foo"

    @machine(output="B")
    def machineB():
        return "bar"

    @machine(inputs=["A", "B"], output="C", requires="all")
    def machineC_all(A, B):
        return A + B

    @machine(inputs=["A", "B"], output="C", requires="any")
    def machineC_any(A, B):
        if A is None:
            return B
        if B is None:
            return A
        return A + B

    with factory(hold=True):
        machineA([1, 2])
        machineB([2, 3])
        tasks_all = machineC_all([1, 2, 3], output_branches="all")
        tasks_any = machineC_any([1, 2, 3], output_branches="any")

    assert len(tasks_all) == 3
    assert tasks_all[0].status.name == "PENDING"
    assert tasks_all[1].status.name == "SUCCESS"
    assert tasks_all[1].output_data == "foobar"
    assert tasks_all[2].status.name == "PENDING"

    assert len(tasks_any) == 3
    assert tasks_any[0].status.name == "SUCCESS"
    assert tasks_any[0].output_data == "foo"
    assert tasks_any[1].status.name == "SUCCESS"
    assert tasks_any[1].output_data == "foobar"
    assert tasks_any[2].status.name == "SUCCESS"
    assert tasks_any[2].output_data == "bar"


def test_alternate_inputs():
    """test using alternative inputs"""

    @machine(output="A")
    def machineA():
        return "A"

    @machine(output="B")
    def machineB():
        return "B"

    @machine(inputs="A|B", output="C")
    def machineC(A):
        return A

    with factory(hold=True):
        machineA(1)
        machineB([1, 2])
        tasks = machineC([1, 2])

    assert tasks[0].status.name == "SUCCESS"
    assert tasks[0].output_data == "A"
    assert tasks[1].status.name == "SUCCESS"
    assert tasks[1].output_data == "B"


def test_branch_fallback():
    """branch fallback : use parent branch"""

    @machine(output="A")
    def machineA():
        pass

    @machine(output="B")
    def machineB():
        pass

    @machine(inputs=["A", "B"], output="C")
    def machineC(A, B, identifier_A, identifier_B):
        return (identifier_A, identifier_B)

    with factory():
        machineA.single("id1")
        machineB.single("id1", "br1")
        task = machineC.single("id1", "br1")
        hold()
        assert task.identifier == (Index("id1"), Branch("br1"))
        assert task.status.name == "SUCCESS"
        assert task.output_data == (("id1", None), ("id1", "br1"))

    # if none of the branch are available, no fallback
    with factory():
        machineA.single("id1")
        machineB.single("id1")
        task = machineC.single("id1", "br1")
        hold()
        assert task.identifier == (Index("id1"), Branch("br1"))
        assert task.status.name == "PENDING"


def test_task_status():
    """test task status attribute"""

    @machine(output="A")
    def machine1():
        pass

    @machine(inputs="A", output="B1")
    def machine2success(A):
        pass

    @machine(inputs="A::unknown", output="B2")
    def machine2pending(A):
        pass

    @machine(inputs="A", output="B3")
    def machine2error(A):
        1 / 0

    @machine(inputs="A", output="B1")
    def machine2skipped(A):
        pass

    with factory(hold=True):
        task1 = machine1()[0]

        task2success = machine2success()[0]
        task2error = machine2error()[0]
        task2pending = machine2pending()[0]
        task2skipped = machine2skipped()[0]

    assert task2success.status == Status.SUCCESS
    assert task2error.status == Status.ERROR
    assert task2pending.status == Status.PENDING
    assert task2skipped.status == Status.SKIPPED


def test_func_arguments():
    """test the various func arguments"""

    # no inputs, no parameters
    tim = []

    @machine(output="A")
    def machine1():
        tim.append(time.time())
        return tim[0]

    @machine(output="B")
    def machine2(identifier_B):
        return identifier_B.index

    # only inputs
    @machine(inputs="A & B", output="C")
    def machine3(A, B):
        return A, B

    # inputs
    @machine(inputs="A & B", output="D", p1=(int, 2))
    def machine4(A, B, p1):
        return A, B, p1

    with factory(hold=True):
        task1 = machine1.single(3)
        task2 = machine2.single(3)
        task3 = machine3.single(3)
        task4 = machine4.single(3)

    assert task1.output_data == tim[0]
    assert task2.output_data == "3"
    assert task3.output_data == (tim[0], "3")
    assert task4.output_data == (tim[0], "3", 2)

    #
    # test parameters

    params = {}

    @machine(p1=(int, 1), p2=[str, str], p3=[2, "foobar"])
    def machine1(p1, p2, p3):
        nonlocal params
        params.update({"p1": p1, "p2": p2, "p3": p3})

    # run machine
    with factory(hold=True):
        task1 = machine1(p2=["a", "b"], p3=2)[0]

    assert params == {"p1": 1, "p2": ["a", "b"], "p3": 2}
    assert task1.index == Index(None)
    assert task1.branch == Branch(None)

    #
    # aggregate

    @machine(output="A")
    def machine1(identifier_A):
        index = identifier_A.index
        branch = identifier_A.branch
        return index, branch

    @machine(inputs="A", output="B")
    def machine2(A):
        return A

    info = {}

    @machine(inputs="B", aggregate=True)
    def machine3(B, identifier_B):
        nonlocal info
        info["index"] = [id.index for id in identifier_B]
        info["branch"] = [id.branch for id in identifier_B]
        info["identifier"] = identifier_B

    # run machine
    with factory(hold=True):
        # create data
        task1 = machine1([1, 2], "a")
        # change branch
        task2 = machine2([1, 2], "a", output_branches=["b", "b"])
        # aggregate
        task3 = machine3([1, 2], "b")[0]

    # check task2 did change branch
    assert all(task.output_data == (task.output.identifier[0], "a") for task in task2)
    assert all(
        task.output.identifier == (task.output.identifier[0], "b") for task in task2
    )

    # check task3 info
    assert info["index"] == ["1", "2"]
    assert info["branch"] == ["b", "b"]
    assert info["identifier"] == [("1", "b"), ("2", "b")]

    #
    # special arguments

    # inputs
    @machine(inputs=["A", "B"], output="C")
    def dummy(inputs):
        return inputs

    with factory(hold=True) as fy:
        fy.write(Target("A", "id"), "foo")
        fy.write(Target("B", "id"), "bar")
        task = dummy.single("id")
    assert task.output_data == {"A": "foo", "B": "bar"}

    # identifier_* and identifiers
    @machine(inputs="A", output="B")
    def dummy(A, identifier_A, identifiers):
        return (identifier_A, identifiers)

    with factory(hold=True) as fy:
        fy.write(Target("A", "id", "br"), "foo")
        task = dummy.single("id", "br")
    assert task.output_data == (("id", "br"), {"A": ("id", "br"), "B": ("id", "br")})

    # index in output
    @machine(output="A")
    def dummy(identifier_A, identifiers):
        return (identifier_A, identifiers)

    with factory(hold=True) as fy:
        task = dummy.single("id", "br")
    assert task.output_data == (("id", "br"), {"A": ("id", "br")})

    # attachments
    @machine(inputs="A", output="B")
    def dummy(A, attachment_A, attachments):
        return (attachment_A, attachments)

    def callback(task, msg=None):
        """attach info to input and output targets"""
        if task.status.name != "RUNNING":
            return
        for target in task.inputs + task.outputs:
            if target == Target("B", "id", "br"):
                target.attach({"foo": "baz"})
            else:
                target.attach({"foo": "bar"})

    with factory(hold=True) as fy:
        fy.write(Target("A", "id", "br"), None)
        task = dummy.single("id", "br", callback=callback)
    assert task.output_data == (
        {"foo": "bar"},
        {"A": {"foo": "bar"}, "B": {"foo": "baz"}},
    )


def test_task_context():
    """test accessing task context"""

    @machine(inputs=["A", "B"], output="C")
    def machineC(A, B):
        context = get_context()
        return context

    with factory(hold=True) as fy:
        fy.write(Target("A", "id1", "br1"), None)
        fy.write(Target("B", "id1", "br1"), None)
        task = machineC.single(
            "id1",
            "br1",
            output_indices="id2",
            output_branches="br2",
            attach={"foo": "bar"},
        )

    assert task.status.name == "SUCCESS"
    context = task.output_data
    assert context.inputs == ["A", "B"]
    assert context.output == "C"
    assert context.targets == {
        "A": Target("A", "id1", "br1"),
        "B": Target("B", "id1", "br1"),
        "C": Target("C", "id2", ("br1", "br2")),
    }
    assert context.indices == {"A": "id1", "B": "id1", "C": "id2"}
    assert context.branches == {"A": "br1", "B": "br1", "C": ("br1", "br2")}
    assert context.attachments == {"C": {"foo": "bar"}, "A": {}, "B": {}}

    # get context outside of task
    with pytest.raises(RuntimeError):
        get_context()

    # aggregatig case

    @machine(inputs="A", output="B", aggregate=True)
    def machineB(A):
        context = get_context()
        return context

    with factory(hold=True) as fy:
        fy.write(Target("A", "id1", "br1"), None)
        fy.write(Target("A", "id2", "br2"), None)
        task = machineB.single(["id1", "id2"], ["br1", "br2"])

    assert task.status.name == "SUCCESS"
    context = task.output_data
    assert context.inputs == ["A"]
    assert context.output == "B"
    assert context.indices == {"A": ["id1", "id2"], "B": None}
    assert context.branches == {"A": ["br1", "br2"], "B": None}


def test_task_callbacks():
    """test callbacks"""

    @machine(output="A")
    def machine_success():
        pass

    @machine(inputs="X")
    def machine_pending(X):
        pass

    @machine(output="A")
    def machine_skipped():
        pass

    @machine()
    def machine_error():
        1 / 0

    db = {}

    def callback(task, msg=None):
        """callback"""
        nonlocal db
        db[task.name] = task.status

    with factory():
        t1 = machine_success(callback=callback)
        t2 = machine_pending(callback=callback)
        t3 = machine_skipped(callback=callback)
        t4 = machine_error(callback=callback)
        hold()

    assert db["machine_success"] == Status.SUCCESS
    assert db["machine_pending"] == Status.PENDING
    assert db["machine_skipped"] == Status.SKIPPED
    assert db["machine_error"] == Status.ERROR


def test_variable_inputs():
    """test setting input at runtime with parameter"""

    @machine(output="A")
    def machineA():
        return "A"

    @machine(output="B")
    def machineB():
        return "B"

    @machine(inputs="TBD", output="C", TBD=Parameter(type=VariableIO(), default="A"))
    def machineC(TBD):
        return TBD

    with factory(hold=True):
        machineA(1)
        machineB(2)
        task1 = machineC.single(1)
        task2 = machineC.single(2, TBD="B")

    assert task1.status.name == "SUCCESS"
    assert task1.output_data == "A"
    assert task2.status.name == "SUCCESS"
    assert task2.output_data == "B"


def test_aggregate():
    """test the various aggregate options"""

    # basic Machines
    @machine(output="A")
    def machineA(identifier_A):
        index = identifier_A.index
        branch = identifier_A.branch
        return index + branch

    # basic Machine 2
    @machine(output="B")
    def machineB(identifier_B):
        index = identifier_B.index
        branch = identifier_B.branch
        return index + branch

    # map maching
    @machine(inputs="A & B", output="C")
    def machine3_map(A, B):
        return A, B

    # aggregating function
    def machine3(A, B):
        return tuple("".join(input) for input in [A, B])

    # aggregating machines
    machine3_all = machine(
        machine3, inputs="A & B", output="C", aggregate=True, requires="all"
    )
    machine3_any = machine(
        machine3, inputs="A & B", output="C", aggregate=True, requires="any"
    )
    machine3_id = machine(machine3, inputs="A & B", output="C", aggregate="index")
    machine3_br = machine(machine3, inputs="A & B", output="C", aggregate="branch")

    with factory(hold=True) as fy:
        # on multiple ids
        machineA(["a", "b"], "x")
        machineB(["b"], ["x", "y"])

        indices = ["a", "b", "b"]
        branches = ["x", "x", "y"]

        # map
        task3_map = machine3_map(indices, branches, output_branches="map")

        # aggregate
        task3_all = machine3_all(indices, branches, output_branches="all")
        task3_any = machine3_any(indices, branches, output_branches="any")
        task3_id = machine3_id(indices, branches, output_branches="id")
        task3_br = machine3_br(indices, branches, output_branches="br")

    # map: only index b,x in both A and B
    assert len(task3_map) == 3
    assert sum([task.status.name == "SUCCESS" for task in task3_map]) == 1
    assert [
        task.output_data for task in task3_map if task.status.name == "SUCCESS"
    ] == [("bx", "bx")]

    # requires all in both inputs
    assert len(task3_all) == 1
    assert task3_all[0].status == Status.SUCCESS
    assert task3_all[0].identifier == (None, "all")
    assert task3_all[0].output_data == ("axbx", "bxby")

    # requires at least one in each input (same result as machine3_all here)
    assert len(task3_any) == 1
    assert task3_any[0].status == Status.SUCCESS
    assert task3_any[0].identifier == (None, "any")
    assert task3_any[0].output_data == ("axbx", "bxby")

    # aggregate indices
    assert len(task3_id) == 2
    assert task3_id[0].identifier == (Index(None), Branch("x", "id"))  # x in branch
    assert task3_id[0].status.name == "SUCCESS"
    assert task3_id[0].output_data == ("axbx", "bx")
    assert task3_id[1].identifier == (Index(None), Branch("y", "id"))  # y in branch
    assert task3_id[1].status.name == "PENDING"

    # aggregate branches
    assert len(task3_br) == 2
    assert task3_br[0].identifier == (Index("a"), Branch("br"))  # a in index
    assert task3_br[0].status.name == "PENDING"
    assert task3_br[1].identifier == (Index("b"), Branch("br"))  # b in index
    assert task3_br[1].status.name == "SUCCESS"
    assert task3_br[1].output_data == ("bx", "bxby")

    # test requires any vs all
    with factory(hold=True):
        machineA(["a", "b"], "x")

        indices = ["a", "a", "b", "b"]
        branches = ["x", "y", "x", "y"]

        task3_all = machine3_all.single(indices, branches, output_branches="all")
        task3_any = machine3_any.single(indices, branches, output_branches="any")

    assert task3_all.status.name == "PENDING"
    assert task3_any.status.name == "SUCCESS"
    assert task3_any.output_data == ("axbx", "")  # no data from b


def test_validation():
    """test input validation through RejectException"""

    @machine(output="A")
    def machineA():
        pass

    @machine(inputs="A")
    def machineB(A):
        pass

    def read_cb(target):
        # read callback
        if int(target.index.values) > 2:
            raise RejectException()

    storageA = MemoryStorage(on_read=read_cb)

    with factory(storages={"A": storageA}):
        # run input machine
        machineA([2, 3])

        # run output machines
        task2 = machineB.single(2)
        task3 = machineB.single(3)
        hold()

    assert task2.status == Status.SUCCESS
    assert task3.status == Status.REJECTED

    # test validation in aggregate machine

    @machine(inputs="A", output="C", aggregate=True)
    def machineC(A, identifier_A):
        return [id.index for id in identifier_A]

    storageA = MemoryStorage(on_read=read_cb)
    with factory(storages={"A": storageA}):
        taskA = machineA([1, 2, 3, 4])
        task_12 = machineC.single([1, 2], output_branches="12")
        task_23 = machineC.single([2, 3], output_branches="23")
        task_34 = machineC.single([3, 4], output_branches="34")
        hold()

    assert all(task.status == Status.SUCCESS for task in taskA)

    assert task_12.status == Status.SUCCESS
    assert task_12.output_data == ["1", "2"]

    assert task_23.status == Status.SUCCESS
    assert task_23.output_data == ["2"]

    assert task_34.status == Status.REJECTED


def test_serialize_task():
    """test serializing task"""

    @machine(output="A", p1=(str, "foo"), p2=["bar", "baz"], p3=Parameter(int, nargs=2))
    def machineA(p1, p2, p3):
        return p1 * p3[0] + p2 * p3[1]

    with factory(hold=True):
        task1 = machineA.single(
            "id1", "br1", output_branches="br2", p2="bar", p3=[2, 3]
        )

    result1 = task1.output_data
    ser = task1.serialize()

    with factory(hold=True):
        task2 = machineA.recall(ser)
        task2.run()

    result2 = task2.output_data
    assert result2 == result1


def test_metamachine_class():
    """test MetaMachine"""

    @machine(output="A")
    def machineA():
        return "bar"

    @machine(inputs="A", output="B", p1=(int, 1))
    def machineB(A, p1):
        return A * p1

    @machine(inputs="B", output="C")
    def machineC(B):
        return "foo" + B

    # create metamachine from list
    meta = metamachine([machineA, machineB, machineC])

    with factory() as fa:
        # default line
        tasks = meta(1, p1=2)
        hold()
        assert all(task.status.name == "SUCCESS" for task in tasks)
        assert tasks[-1].output_data == "foobarbar"

    # check inputs and outputs
    meta = metamachine([machineB, machineC])
    assert meta.inputs == {"A": [Input("A")]}
    assert meta.outputs == {"C": [Output("C")]}
    assert meta.parameters == {"p1": Parameter(int, default=1)}
    assert meta.output is None

    # multi input metamachine
    machineMulti = Machine(lambda A, B: None, inputs="A & B")
    meta = metamachine([machineMulti])
    assert meta.inputs == {"A": [Input("A")], "B": [Input("B")]}
    assert meta.outputs == {}

    # create metamachine from function
    @metamachine(p1=(int, 1), output=["A", "C"])
    def meta(p1):
        if p1 == 1:
            return machineA
        else:
            return [machineA, machineB, machineC]

    assert meta.inputs == {}
    assert meta.outputs == {"A": [Output("A")], "C": [Output("C")]}

    with factory() as fy:
        # default line
        tasks1 = meta([1, 2])
        tasks2 = meta(2, p1=2)
        tasks3 = meta(3, p1=2)
        hold()

        assert not fy.exists(Target("B", 1))
        assert fy.read(Target("C", 2)) == "foobarbar"
        assert fy.read(Target("C", 3)) == "foobarbar"

        # rerun case 3 with different parameters
        tasks3 = meta(3, p1=3)
        hold()

        # results were not overwritten
        assert fy.read(Target("C", 3)) == "foobarbar"

        # rerun case 2 with different parameters
        tasks2 = meta(3, p1=3, overwrite=True)
        hold()

        # results were overwritten
        assert fy.read(Target("C", 3)) == "foobarbarbar"

    # metamachine from dict

    # check inputs and outputs
    meta = metamachine({"a": machineB, "b": machineC})
    assert meta.inputs == {"A": [Input("A")], "B": [Input("B")]}
    assert meta.outputs == {"B": [Output("B")], "C": [Output("C")]}
    assert set(meta.parameters) == {"p1", "choice"}

    @machine(output="D")
    def machineD():
        return "wahou"

    meta1 = metamachine({"a": [machineA, machineB, machineC], "b": machineD})
    with factory() as fa:
        tasks_a = meta1(1, choice="a", p1=2)
        tasks_b = meta1(2, choice="b")
        hold()
        assert fa.read(Target("C", 1)) == "foobarbar"
        assert fa.read(Target("D", 2)) == "wahou"

    # metamachine of metamachine

    meta1 = metamachine([machineA, machineB, machineC])
    meta2 = metamachine({"a": meta1, "b": machineD})
    with factory() as fa:
        tasks_a = meta2(1, choice="a", p1=2)
        tasks_b = meta2(2, choice="b")
        hold()
        assert fa.read(Target("C", 1)) == "foobarbar"
        assert fa.read(Target("D", 2)) == "wahou"

    # metamachine creating arbitrary machine

    @metamachine(inputs=["A", "B"], output=["X", "Y"], p=["A", "B"], suffix=str)
    def meta(p):
        if p == "A":

            @machine(inputs="A", output="X", suffix=str)
            def myMachine(A, suffix):
                return A.upper() + suffix

        if p == "B":

            @machine(inputs="B", output="Y")
            def myMachine(B):
                return B.upper()

        return myMachine

    assert meta.inputs == {"A": [Input("A")], "B": [Input("B")]}
    assert meta.outputs == {"X": [Output("X")], "Y": [Output("Y")]}

    with factory() as fy:
        machineA([1, 2])
        machineB(2, p1=2)
        tasks_a = meta(1, p="A", suffix="_foobar")
        tasks_b = meta(2, p="B", suffix="")
        fy.hold()
        assert fy.read(Target("X", 1)) == "BAR_foobar"
        assert fy.read(Target("Y", 2)) == "BARBAR"


def test_metamachine_identifiers():
    """test metamachine indentifier order"""

    @machine(output="A")
    def machineA(p1=Parameter(str)):
        return p1

    @machine(inputs="A", output="B")
    def machineB(A):
        return A.upper()

    # non aggregating metamachine
    meta1 = metamachine([machineA, machineB])

    with factory(hold=True):
        tasks1 = meta1("id1", output_indices="id2", p1="foobar")
        tasks2 = meta1("id1", output_indices="id2", output_branches="br1", p1="foobaz")

    assert all(task.status.name == "SUCCESS" for task in tasks1)
    assert all(task.status.name == "SUCCESS" for task in tasks2)
    assert tasks1[-1].output_data == "FOOBAR"
    assert tasks2[-1].output_data == "FOOBAZ"

    # check indices
    assert tasks1[0].identifier == ("id2", None)
    assert tasks1[1].identifier == ("id2", None)  # new output index

    assert tasks2[0].identifier == ("id2", "br1")  # new output branch
    assert tasks2[1].identifier == ("id2", "br1")  # new output index

    #
    # aggregating case

    @machine(output="A", p1=Parameter(str))
    def machineA(p1):
        return p1

    @machine(inputs="A", output="B", aggregate=True)
    def machineB(A):
        return A

    # aggregating metamachine
    meta2 = metamachine([machineA, machineB])

    with factory(hold=True):
        indices = ["id11", "id12", "id13"]
        tasks1 = meta2(indices, output_indices="id2", p1="foobar")
        tasks2 = meta2(
            indices, output_indices="id2", output_branches="br1", p1="foobaz"
        )

    # targets A were not recomputed the second time
    assert tasks1[-1].output_data == ["foobar", "foobar", "foobar"]
    assert tasks2[-1].output_data == ["foobar", "foobar", "foobar"]

    # check indices
    assert tasks1[0].identifier == ("id11", None)
    assert tasks1[1].identifier == ("id12", None)
    assert tasks1[2].identifier == ("id13", None)
    assert tasks1[3].identifier == ("id2", None)  # new output index

    assert tasks2[0].identifier == ("id11", None)
    assert tasks2[1].identifier == ("id12", None)
    assert tasks2[2].identifier == ("id13", None)
    assert tasks2[3].identifier == ("id2", "br1")  # new output id/br


def test_metamachine_aggregate():
    """test metamachine with aggregate sequence"""

    @machine(output="A")
    def machineA(identifier_A):
        return identifier_A.index

    @machine(inputs="A", output="B", aggregate=True, requires="any")
    def machineB(A):
        return "x".join(A)

    @machine(inputs="B", output="C")
    def machineC(B):
        return B.upper()

    meta = metamachine([machineA, machineB, machineC])

    with factory() as fy:
        tasks = meta([1, 2, 3], output_indices="a", output_branches="x")
        hold()

    assert all(task.status == Status.SUCCESS for task in tasks)
    assert tasks[-1].output_data == "1X2X3"


def test_graph():
    """test dependency graph"""

    @machine(output="A")
    def machineA():
        pass

    @machine(inputs="A", output="B")
    def machineB(A):
        pass

    @machine(inputs="B", output="C")
    def machineC(B):
        pass

    with factory(hold=True):
        taskA = machineA.single(1)
        taskB = machineB.single(1)

    assert taskA.output.parents == []
    assert taskB.output.parents == [Target("A", 1)]

    # with multiple machines
    meta = metamachine([machineA, machineB, machineC])
    storages = {"B": MemoryStorage(temporary=True)}
    with factory(hold=True, storages=storages):
        tasks1 = meta(1)

    assert tasks1[-1].output == Target("C", 1)
    assert set(tasks1[-1].output.parents) == {Target("A", 1), Target("B", 1)}

    # test replay
    history = tasks1[-1].history

    with factory(hold=True):
        tasks2 = meta.replay(history)
    assert all(t1.output == t2.output for t1, t2 in zip(tasks1, tasks2))

    # non-aggregating metamachine
    @machine(output="A")
    def machineA():
        return get_context().targets["A"]

    @machine(inputs="A1 & A2", output="B")
    def machineB(A1, A2):
        return (A1, A2)

    @metamachine(output="B")
    def meta():
        return [machineA.copy(output="A::A1"), machineA.copy(output="A::A2"), machineB]

    with factory(hold=True):
        tasks = meta("id1", output_indices="id2", output_branches="br2")
    assert tasks[-1].status.name == "SUCCESS"
    assert tasks[-1].output_data == (
        Target("A1", "id2", "br2"),
        Target("A2", "id2", "br2"),
    )
    assert tasks[-1].output == Target("B", "id2", "br2")

    # aggregating metamachine
    @machine(output="A")
    def machineA():
        return get_context().indices["A"]

    @machine(inputs="A", output="B", aggregate=True)
    def machineB(A):
        return A

    @machine(inputs="B1 & B2", output="C")
    def machineC(B1, B2):
        return {"B1": B1, "B2": B2}

    @metamachine(output="C")
    def meta():
        return [
            machineA.copy(output="A::A1"),
            machineB.copy(inputs="A::A1", output="B::B1"),
            machineA.copy(output="A::A2"),
            machineB.copy(inputs="A::A2", output="B::B2"),
            machineC,
        ]

    with factory(hold=True):
        tasks = meta(["id1", "id2"], output_indices="id3", output_branches="br1")
    assert tasks[-1].status.name == "SUCCESS"
    assert tasks[-1].output == Target("C", "id3", "br1")
    assert tasks[-1].output_data == {"B1": ["id1", "id2"], "B2": ["id1", "id2"]}


def test_freeze_parameters():
    """test freeze parameters"""

    @machine(output="A", param1=str, param2=str)
    def machine1(param1, param2):
        return param1 + param2

    machine2 = machine(machine1, param2=Freeze("bar"))

    assert "param1" in machine2.parameters
    assert not "param2" in machine2.parameters

    with factory(hold=True):
        task = machine2.single(param1="foo")

    assert task.output_data == "foobar"


def test_indexwise_parameters():
    """test index-wise parameters"""

    @machine(output="A", p1=str)
    def machine1(p1):
        return p1

    @machine(inputs="A", output="B", p2=str, aggregate=True)
    def machine2(A, p2):
        return "|".join([p2 + a for a in A])

    with factory(hold=True):
        identifiers = [("id1", "br1"), ("id2", "br1")]
        # one parameter value per index
        parameters = {identifiers[0]: {"p1": "bar"}, identifiers[1]: {"p1": "baz"}}
        tasks_1 = machine1(identifiers=identifiers, parameters=parameters)
        task_2 = machine2.single(identifiers=identifiers, p2="foo")

    assert tasks_1[0].status.name == "SUCCESS"
    assert tasks_1[1].status.name == "SUCCESS"
    assert tasks_1[0].output_data == "bar"
    assert tasks_1[1].output_data == "baz"

    assert task_2.status.name == "SUCCESS"
    assert task_2.output_data == "foobar|foobaz"

    with factory(hold=True):
        with pytest.raises(ValueError):
            # missing one index in p1
            parameters = {("id1", None): {"p1": "bar"}}
            tasks = machine1(["id1", "id2"], parameters=parameters)
