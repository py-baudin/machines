""" machines decorators """

from .machine import Machine, MetaMachine
from .io import TargetType
from . import parameters


def machine(obj=None, **kwargs):
    """helper function/decorator for creating a Machine"""

    if isinstance(obj, Machine):
        return obj.copy(**kwargs)
    elif callable(obj):
        return _setup_machine(obj, Machine, **kwargs)
    elif obj:
        raise ValueError(f"Invalid Machine initializer: {obj}")

    # decorator with kwargs
    def wrapper(obj):
        return _setup_machine(obj, Machine, **kwargs)

    return wrapper


def metamachine(obj=None, **kwargs):
    """helper function/decorator for creating a MetaMachine"""
    if isinstance(obj, MetaMachine):
        return obj.copy(**kwargs)

    elif isinstance(obj, Machine):
        # obj is used to initialize the MetaMachine
        return MetaMachine.from_list([obj], **kwargs)

    elif isinstance(obj, list):
        # obj is used to initialize the MetaMachine
        return MetaMachine.from_list(obj, **kwargs)

    elif isinstance(obj, dict):
        # obj is used to initialize the MetaMachine
        return MetaMachine.from_dict(obj, **kwargs)

    elif callable(obj):
        # obj is a function
        return _setup_machine(obj, MetaMachine, **kwargs)

    elif obj:
        raise ValueError(f"Invalid MetaMachine initializer: {obj}")

    # else: decorator
    def wrapper(func):
        # metamachine is used as decorator
        return _setup_machine(func, MetaMachine, **kwargs)

    return wrapper


def input(
    name,
    dest=None,
    *,
    type=None,
    group=None,
    handler=None,
    temp=False,
    replace=False,
    variable=False,
    **kwargs,
):
    """add input to the current machine

    Args:
        name: i/o's name in function
        dest: target's name
        group: target's group
        type: target's type
        handler: file_handler
        temp: is target temporary?
        replace: replace previously defined i/o
        variable: is target variable?
        if variable is True:
            default: default target
            none: is None an acceptable value?
            help: variable target help/description
    """
    if not dest:
        dest = name
    name = str(name)

    # variable io
    is_variable = variable or not isinstance(dest, (str, TargetType))
    if is_variable:
        param_type = parameters.setup_variable_io(dest, type=type, handler=handler)
        parameter = parameters.setup_parameter(param_type, name=name, **kwargs)
        dest, type, handler, temp = Ellipsis, None, None, False  # virtual target type

    # init TargetType
    if not isinstance(dest, TargetType):
        dest = TargetType(dest, type, handler=handler, temp=temp)

    def set_input(func):
        if is_variable:
            _store_init(
                func,
                "parameters",
                {"name": name, "parameter": parameter, "replace": replace},
            )

        _store_init(
            func,
            "inputs",
            {"name": name, "inputs": dest, "group": group, "replace": replace},
        )
        return func

    return set_input


def output(
    name,
    dest=None,
    *,
    type=None,
    handler=None,
    temp=False,
    replace=False,
    variable=False,
    **kwargs,
):
    """add output to the current machine

    Args:
        name: i/o's name in function
        dest: target's name
        type: target's type
        handler: file_handler
        temp: is target temporary?
        replace: replace previously defined i/o
        variable: is target variable?

        if variable is True:
            default: default target
            none: is None an acceptable value?
            help: variable target help/description
    """
    if not dest:
        dest = name
    name = str(name)

    # variable io
    is_variable = variable or not isinstance(dest, (str, TargetType))
    if is_variable:
        param_type = parameters.setup_variable_io(dest, type=type, handler=handler)
        parameter = parameters.setup_parameter(param_type, name=name, **kwargs)
        dest, type, handler, temp = Ellipsis, None, None, False  # virtual target type

    # init TargetType
    if not isinstance(dest, TargetType):
        dest = TargetType(dest, type, handler=handler, temp=temp)

    def set_output(func):
        if is_variable:
            _store_init(
                func,
                "parameters",
                {"name": name, "parameter": parameter, "replace": replace},
            )

        _store_init(
            func, "outputs", {"name": name, "outputs": dest, "replace": replace}
        )
        return func

    return set_output


def parameter(name, type=None, *, help=None, replace=False, **kwargs):
    """add parameter to the current machine"""

    if "description" in kwargs and not help:
        help = kwargs.pop("description")

    parameter = parameters.setup_parameter(type, name=name, help=help, **kwargs)

    def set_parameter(func):
        _store_init(
            func,
            "parameters",
            {"name": name, "parameter": parameter, "replace": replace},
        )
        return func

    return set_parameter


def _setup_machine(obj, type, **kwargs):
    """create machine object from multiple decorators"""
    if isinstance(obj, Machine):
        raise TypeError(f"Machine {obj} already initialized")
    machine = type(obj, **kwargs)

    # add input/output/params from other decorators
    init = getattr(obj, "__machine_params__", {})
    for item in init.get("inputs", [])[::-1]:
        machine.set_input(**item)
    for item in init.get("outputs", [])[::-1]:
        machine.set_output(**item)
    for item in init.get("parameters", [])[::-1]:
        machine.set_parameter(**item)

    return machine


def _store_init(func, type, item):
    """store parameters for later"""
    if not hasattr(func, "__machine_params__"):
        func.__machine_params__ = {}
    func.__machine_params__.setdefault(type, []).append(item)
