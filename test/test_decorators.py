import machines
from machines import parameters
import pathlib


def test_machine_decorators():

    # dummy
    @machines.machine()
    @machines.output("data_A")
    def setup1():
        return "foo"

    # basic setup

    @machines.machine()
    @machines.input("A", dest="data_A", type="type_A")
    @machines.output(
        "data_B",
        type="type_B",
    )
    @machines.parameter("param1", int, default=None, help="Parameter 1")
    @machines.parameter("param2", ["bar", "baz"])
    def machine1(A, param1, param2):
        return A + param2

    assert isinstance(machine1, machines.Machine)
    assert machine1.aggregate == False
    assert machine1.requires == "all"
    assert machine1.inputs == {"A": [machines.TargetType("data_A")]}
    assert machine1.outputs == {
        "data_B": [machines.TargetType("data_B", type="type_B")]
    }

    assert machine1.parameters["param1"].type is parameters.INT
    assert machine1.parameters["param1"].required == False
    assert machine1.parameters["param1"].default is None
    assert machine1.parameters["param1"].help == "Parameter 1"
    assert isinstance(machine1.parameters["param2"].type, parameters.Choice)
    assert machine1.parameters["param2"].type.values == ("bar", "baz")
    assert machine1.parameters["param2"].required == True

    with machines.factory(hold=True):
        setup1.single("id1")
        task1 = machine1.single("id1", param2="baz")
    assert task1.output_data == "foobaz"

    # more complicated

    handlerA = machines.file_handler(load=lambda dirname: None)

    @machines.machine()
    @machines.input("A", "data_A1")  # alternative inputs
    @machines.input("A", "data_A2", handler=handlerA)
    @machines.output("B")
    def machine2(A):
        return A + "bar"

    assert machine2.inputs == {
        "A": [machines.TargetType("data_A1"), machines.TargetType("data_A2")]
    }
    assert machine2.main_inputs == [machines.TargetType("data_A1")]
    assert machine2.inputs["A"][1].handler is handlerA

    @machines.machine()
    @machines.output("A", variable=True, default=None, help="Variable output A")
    def setup2(A):
        if A.dest == "data_A1":
            return "Foo"
        return "foo"  # else

    assert setup2.parameters["A"].help == "Variable output A"
    assert isinstance(setup2.parameters["A"].type, machines.VariableIO)

    with machines.factory(hold=True):
        setup2.single("id0")
        setup2.single("id1", A="data_A1")
        setup2.single("id2", A="data_A2")
        task0 = machine2.single("id0")
        task1 = machine2.single("id1")
        task2 = machine2.single("id2")

    assert task0.status.name == "PENDING"  # no output in setup2
    assert task1.output_data == "Foobar"
    assert task2.output_data == "foobar"

    # metamachine
    @machines.metamachine
    @machines.output("B")
    @machines.parameter("param2", ["bar", "baz"])
    def meta1(param2):
        return [setup1, machine1]

    with machines.factory(hold=True):
        tasks = meta1("id1", param2="baz")
    assert tasks[-1].output_data == "foobaz"

    # custom parameters
    @machines.machine
    @machines.output("Temp")
    @machines.parameter("flag", is_flag=True)
    @machines.parameter("choice", type=parameters.Choice(["bar", "baz"]))
    @machines.parameter("path", type=parameters.Path(exists=False))
    @machines.parameter("config", type=parameters.Config())
    @machines.parameter("integer", type=int)
    @machines.parameter("integer2", int)
    def machine(flag, choice, path, config, integer, integer2):
        return {
            "flag": flag,
            "choice": choice,
            "path": path,
            "config": config,
            "integer": integer,
            "integer2": integer2,
        }

    assert isinstance(machine.parameters["flag"].type, machines.Flag)
    assert machine.parameters["flag"].default == False
    assert machine.parameters["flag"](True) == True

    assert isinstance(machine.parameters["path"].type, machines.Path)
    assert isinstance(machine.parameters["config"].type, machines.Config)

    assert isinstance(machine.parameters["choice"].type, machines.Choice)
    assert machine.parameters["choice"].type.values == ("bar", "baz")
    assert machine.parameters["choice"]("bar") == "bar"

    assert machine.parameters["integer"].type is machines.INT
    assert machine.parameters["integer"](5) == 5

    with machines.factory(hold=True):
        task = machine.single(
            flag=True,
            path="my/path",
            choice="baz",
            config={"con": "fig"},
            integer=5,
            integer2=6,
        )

    path = pathlib.Path("my") / "path"
    assert task.output_data == {
        "flag": True,
        "path": str(path),
        "choice": "baz",
        "config": {"con": "fig"},
        "integer": 5,
        "integer2": 6,
    }
