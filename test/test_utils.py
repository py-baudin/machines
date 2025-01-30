# -*- coding: utf-8 -*-
""" test common functions """

import os
import json
import pytest

from machines import utils, target, version


def test_id_to_string():
    """test id to string"""
    assert "_" == utils.id_to_string(None)
    assert "foobar" == utils.id_to_string("foobar")
    assert "foo.bar" == utils.id_to_string(("foo", "bar"), sep=".")
    assert "{foo.bar}" == utils.id_to_string(
        ("foo", "bar"), sep=".", delim="{}", nodelim=False
    )
    assert "foo.{bar.baz}" == utils.id_to_string(
        ("foo", ("bar", "baz")), sep=".", delim="{}"
    )

    with pytest.raises(ValueError):
        utils.id_to_string(1)

    with pytest.raises(ValueError):
        utils.id_to_string(["foobar"])

    with pytest.raises(ValueError):
        utils.id_to_string(("foobar", 1))


def test_id_from_string():
    """test id from string"""
    assert None == utils.id_from_string("_")
    assert "foobar" == utils.id_from_string("foobar")
    assert ("foo", "bar") == utils.id_from_string("foo.bar", sep=".")
    assert ("foo", ("bar", "baz")) == utils.id_from_string(
        "foo.{bar.baz}", sep=".", delim="{}"
    )

    with pytest.raises(ValueError):
        utils.id_from_string("{foo.bar")

    with pytest.raises(ValueError):
        utils.id_from_string("foo.{{bar.baz}")


def test_index_compare():
    """ """
    T = target.Target

    # sort list of targets
    s = sorted(
        [T("A"), T("B", "a"), T("C", "a", "y"), T("D", "b", "x"), T("E", "a")],
        key=utils.indices_as_key,
    )
    assert s == [T("B", "a"), T("E", "a"), T("C", "a", "y"), T("D", "b", "x"), T("A")]


def test_signature(tmpdir):
    dest = tmpdir.mkdir("signature")
    presets = {key[1:]: key for key in utils.Signature.PRESETS}
    custom2 = lambda dirname: dirname
    sign = utils.Signature(".foobar", custom1="foobar", custom2=custom2, **presets)

    # add file
    with open(dest / "dummy", "w") as fp:
        fp.write("foobar")

    # exec signature
    now = utils.datetime.datetime.now()
    sign(dest)

    assert (dest / ".foobar").isfile()
    with open(dest / ".foobar") as fp:
        values = json.load(fp)

    assert values["custom1"] == "foobar"
    assert values["custom2"] == str(dest)
    assert values["DATETIME"] == now.strftime("%Y%m%d-%H%M%S")
    assert values["DATE"] == now.date().strftime("%Y%m%d")
    assert values["MACHINES"] == version.__version__
    assert values["USER_LOGIN"] == utils.getpass.getuser()
    assert values["FILES"] == ["dummy"]
    assert values["HASH"] == {"dummy": utils.hash_file(dest / "dummy")}
