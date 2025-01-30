# -*- coding: utf-8 -*-
""" test io """
import pytest

from machines.target import Branch, Index, Target
from machines.io import TargetType, Input, Output, parse_io, parse_string_io


def test_io_class():
    io = TargetType("A")
    assert io.type is None
    assert io.dest == "A"

    io = TargetType("A", type="Atype")
    assert io.dest == "A"
    assert io.type == "Atype"

    # compare ios
    assert TargetType("A") == TargetType("A")
    assert TargetType("A") != TargetType("B")
    assert TargetType("A", "T1") == TargetType("A", "T2")

    with pytest.raises(TypeError):
        TargetType(1)

    with pytest.raises(ValueError):
        TargetType("a b")

    # undefined io
    io = TargetType(...)
    assert io.is_virtual
    assert not io.update(dest="A").is_virtual


def test_io_target():
    """test Target creation"""

    target = Input("A").target(None)
    assert target == Target("A")

    target = Input("A").target("foo")
    assert target == Target("A", "foo")

    target = Input("A").target("foo", "branch")
    assert target == Target("A", "foo", "branch")

    # multi id / branch
    target = Input("A").target(branch=("b1", "b2"))
    assert target == Target("A", branch=("b1", "b2"))

    target = Input("A").target(Index("foo", "bar"))
    assert target == Target("A", ("foo", "bar"))

    # types
    target = Input("A", type="Atype").target("foo")
    assert target.name == "A"
    assert target.type == "Atype"

    # variable io
    with pytest.raises(RuntimeError):
        Input(...).target("id")


#
def test_parse_string_io():

    # basic
    name, io = parse_string_io("A")
    assert name == "A"
    assert io.type is None
    assert io.dest == "A"

    # with type
    name, io = parse_string_io("A:T")
    assert name == "A"
    assert io.type == "T"
    assert io.dest == "A"

    # with dest
    name, io = parse_string_io("A::B")
    assert name == "A"
    assert io.dest == "B"
    assert io.type is None

    # with type and dest
    name, io = parse_string_io("A:T:B")
    assert name == "A"
    assert io.dest == "B"
    assert io.type == "T"

    with pytest.raises(ValueError):
        parse_string_io("A B")

    with pytest.raises(ValueError):
        parse_string_io("A()")


def test_parse_io():
    inputs = parse_io("A")
    assert inputs == {"A": [Input("A")]}

    inputs = parse_io("A & B")
    assert inputs == {"A": [Input("A")], "B": [Input("B")]}

    inputs = parse_io("A | B")
    assert inputs == {"A": [Input("A"), Input("B")]}

    inputs = parse_io("A::A1 | A2")
    assert inputs == {"A": [Input("A1"), Input("A2")]}

    with pytest.raises(ValueError):
        parse_io("A::A1 | B::B1")

    with pytest.raises(ValueError):
        parse_io("A | B", allow_alts=False)

    inputs = parse_io(["A", "B"])
    assert inputs == {"A": [Input("A")], "B": [Input("B")]}

    # multiple inputs with type
    inputs = parse_io("A:T & B:T")
    assert inputs == {"A": [Input("A")], "B": [Input("B")]}
    assert inputs["A"][0].type == "T"
    assert inputs["B"][0].type == "T"

    # multiple inputs with dest
    inputs = parse_io("A::A1 & B::B1")
    assert inputs == {"A": [Input("A1")], "B": [Input("B1")]}

    # multiple inputs with secondary dest
    inputs = parse_io("A::A1 & B1|B2")
    assert inputs == {
        "A": [TargetType("A1")],
        "B1": [TargetType("B1"), TargetType("B2")],
    }

    # list
    inputs = parse_io(["A::A1", "B::B1"])
    assert inputs == {"A": [Input("A1")], "B": [Input("B1")]}

    inputs = parse_io(["A::A1", "A::A2"])
    assert inputs == {"A": [Input("A1"), Input("A2")]}

    inputs = parse_io(["A", "B1|B2"])
    assert inputs == {"A": [Input("A")], "B1": [Input("B1"), Input("B2")]}

    # dict
    inputs = parse_io({"A": Input("A1")})
    assert inputs == {"A": [Input("A1")]}

    inputs = parse_io({"A": [Input("A1"), Input("A2")]})
    assert inputs == {"A": [Input("A1"), Input("A2")]}

    with pytest.raises(ValueError):
        parse_io({"A": "A1"})

    with pytest.raises(ValueError):
        parse_io({"A": ["A1", "A2"]})

    # groups
    # inputs = parse_io([Input("A1", group="A"), Input("A2", group="A")])
    # assert [input.name for input in inputs] == ["A1", "A2"]
    # assert [input.group for input in inputs] == ["A", "A"]
    #
    # inputs = parse_io({"A": "A1 & A2"})
    # assert [input.name for input in inputs] == ["A1", "A2"]
    # assert [input.group for input in inputs] == ["A", "A"]
