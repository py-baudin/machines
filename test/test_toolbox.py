# -*- coding: utf-8 -*-
""" test toolbox class """

import pytest
from machines.machine import Machine
from machines.decorators import machine, metamachine
from machines.toolbox import Toolbox
from machines.handlers import file_handler


def test_toolbox():
    """test Toolbox class"""

    # create program
    @machine(output="A", p1=(int, 1))
    def MachineA(p1):
        pass

    # make toolbox
    toolbox = Toolbox("my-program1", description="does something")

    # make process from list
    toolbox.add_program("prog1", [MachineA])

    assert toolbox.name == "my-program1"
    assert "prog1" in toolbox
    assert isinstance(toolbox["prog1"], Machine)
    assert toolbox.info["name"] == "my-program1"
    assert toolbox.info["description"] == "does something"
    assert toolbox.info["programs"][0]["name"] == "prog1"


def test_toolbox_io():
    """test toolbox io handling"""

    @machine(inputs="A::destA", output="destB:typeB")
    def Machine1(A):
        pass

    @machine(inputs="A:typeA:destA", output="destC")
    def Machine2(A):
        pass

    toolbox = Toolbox("tb")
    toolbox.add_program("prog1", [Machine1])
    toolbox.add_program("prog2", [Machine2])

    handler_destA = file_handler(save=lambda dest, dir: None)
    handler_typeB = file_handler(save=lambda dest, dir: None)
    toolbox.add_handlers({"destA": handler_destA, "typeB": handler_typeB})

    # ios = toolbox.ios
    # # assert ios == {"destA": "typeA", "destB": "typeB", "destC": None}
    # assert ios == ["destA", "destB", "destC"]

    handlers = toolbox.handlers
    assert handlers == {
        "destA": handler_destA,
        # "destB": "handler_typeB",
        "typeB": handler_typeB,
    }

    # @machine(inputs="destA:typeX")  # destA already has a type
    # def Machine3(destA):
    #     pass
    #
    # with pytest.raises(ValueError):
    #     toolbox.add_program("prog3", [Machine3])
