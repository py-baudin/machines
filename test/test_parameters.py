# -*- coding: utf-8 -*-
""" test parameters """
import os
import tempfile
import pytest
import json
import yaml

from machines import parameters, io


def test_base_parameter_classes():

    assert parameters.STRING("foobar") == "foobar"
    assert parameters.BOOL(0) == False
    assert parameters.INT(42) == 42
    assert parameters.FLOAT(42.3) == 42.3

    with pytest.raises(parameters.ParameterError):
        parameters.INT("foobar")

    # multiple
    type = parameters.BaseType(float, str)
    assert type(42) == 42.0
    assert type("foobar") == "foobar"


def test_flag_class():
    flag = parameters.Flag()
    assert flag(1) == True
    assert flag("True") == True
    assert flag(False) == False
    assert flag("False") == False

    with pytest.raises(parameters.ParameterError):
        flag("foobar")


def test_switch_class():
    switch = parameters.Switch(a="foobar", b="foobaz")
    assert switch("a") == "foobar"
    assert switch("b") == "foobaz"

    with pytest.raises(parameters.ParameterError):
        switch("c")


def test_path_class(tmpdir):
    path = parameters.Path(exists=True)
    assert path(tmpdir) == tmpdir

    with pytest.raises(parameters.ParameterError):
        path(tmpdir / "wrong")


def test_config_class(tmpdir):
    preset1 = {"a": "foobar"}
    with open(tmpdir / "preset1.yml", "w") as fp:
        yaml.dump(preset1, fp)

    config = parameters.Config(presets=tmpdir)
    assert config("preset1") == preset1
    assert config(tmpdir / "preset1.yml") == preset1

    # filename attribute
    assert config("preset1").filename == tmpdir / "preset1.yml"
    assert config(tmpdir / "preset1.yml").filename == tmpdir / "preset1.yml"

    with pytest.raises(parameters.ParameterError):
        config("wrong")
    with pytest.raises(parameters.ParameterError):
        config(tmpdir / "wrong")


def test_parameter_class():
    Parameter = parameters.Parameter

    # base types
    param = Parameter(parameters.STRING, name="p")
    assert param.name == "p"
    assert param.nargs is None
    assert param.type is parameters.STRING
    assert param.parse("foobar") == "foobar"

    with pytest.raises(parameters.ParameterError):
        param.parse(None)  # wrong type

    param = Parameter(parameters.STRING, none=True)
    assert param.parse("foobar") == "foobar"
    assert param.parse(None) == None

    # multiple
    param = Parameter(parameters.INT, nargs=2)
    assert param.nargs == 2
    assert param.type is parameters.INT
    assert param.parse([1, 2]) == [1, 2]

    with pytest.raises(parameters.ParameterError):
        param.parse("foobar")  # wrong type

    with pytest.raises(parameters.ParameterError):
        param.parse(1)  # wrong number

    with pytest.raises(parameters.ParameterError):
        param.parse([1, 2, 3])  # wrong number

    # unlimited number
    param = Parameter(parameters.INT, nargs=-1)
    assert param.nargs == -1
    assert param.type is parameters.INT
    assert param.parse([1, 2]) == [1, 2]
    assert param.parse([1, 2, 3]) == [1, 2, 3]

    # default
    param = Parameter(parameters.FLOAT, default=2)
    assert param.parse() == 2.0  # default
    assert param.parse(3) == 3.0

    param = Parameter(parameters.FLOAT, default=None)
    assert param.parse() is None
    assert param.parse(None) is None
    assert param.parse(2.0) == 2.0

    # flag
    param = Parameter(parameters.Flag())
    assert param.parse(1)
    assert not param.parse("false")

    with pytest.raises(parameters.ParameterError):
        param.parse("foobar")

    # switch
    param = Parameter(parameters.Switch(a="bar", b="baz"))
    assert param.parse("a") == "bar"
    assert param.parse("b") == "baz"

    with pytest.raises(parameters.ParameterError):
        param.parse("c")

    # choice
    param = Parameter(parameters.Choice(["bar", "baz"]))
    assert param.parse("bar") == "bar"
    assert param.parse("baz") == "baz"

    with pytest.raises(parameters.ParameterError):
        param.parse("foo")

    # Variable io
    TargetType = io.TargetType
    VariableIO = parameters.VariableIO
    param = Parameter(parameters.VariableIO(type="output_type"))
    assert param.parse("output1").dest == "output1"
    assert param.parse("output1").type == "output_type"
    assert param.parse(TargetType("output1")).dest == "output1"
    assert param.parse(TargetType("output1")).type is None

    # Variable selector
    VariableSelector = parameters.VariableSelector
    param = Parameter(VariableSelector(["A", "B"]))
    assert param.parse("A").dest == "A"
    assert param.parse("B").dest == "B"
    assert param.parse("A").type is None
    assert param.parse("A").handler is None
    with pytest.raises(parameters.ParameterError):
        param.parse("C")

    param = Parameter(
        VariableSelector({"a": TargetType("A"), "b": "B"}, type="output_type")
    )
    assert param.parse("a").dest == "A"
    assert param.parse("a").type is None
    assert param.parse("a").handler is None
    assert param.parse("b").dest == "B"
    assert param.parse("b").type == "output_type"
    assert param.parse("b").handler is None

    param = Parameter(VariableSelector({True: "A"}), default=None)
    assert param.parse(True).dest == "A"
    assert param.parse(None) is None


def test_setup_parameter():
    param = parameters.setup_parameter(str)
    assert param.type is parameters.STRING
    assert param.nargs is None

    param = parameters.setup_parameter(int)
    assert param.type is parameters.INT

    param = parameters.setup_parameter("float")
    assert param.type is parameters.FLOAT

    param = parameters.setup_parameter([int, float])
    assert isinstance(param.type, parameters.BaseType)
    assert param.type.types == (int, float)

    param = parameters.setup_parameter(is_flag=True, name="p")
    assert isinstance(param.type, parameters.Flag)
    assert param.type.enable == "p"

    param = parameters.setup_parameter({"a": "foo", "b": "bar"})
    assert isinstance(param.type, parameters.Switch)
    assert param.parse("a") == "foo"
    assert param.parse("b") == "bar"
