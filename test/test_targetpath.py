# -*- coding: utf-8 -*-
""" test common functions """
import os
import pathlib
import datetime
from os.path import join
import pytest

from machines.target import Target
from machines.targetpath import *


def test_target_converter():
    """test converting target to path"""
    # target to path
    conv = TargetToPath()

    path = conv.to_path(Target("name"))
    assert path == join("_", "name")

    path = conv.to_path(Target("name", "id"))
    assert path == join("id", "name")

    path = conv.to_path(Target("name", ("id1", "id2")))
    assert path == join("id1", "id2", "name")

    path = conv.to_path(Target("name", ("id1", ("id2", "id3"))))
    assert path == join("id1", "{id2.id3}", "name")

    path = conv.to_path(Target("name", None, "branch"))
    assert path == join("_", "name~branch")

    path = conv.to_path(Target("name", "id", "branch"))
    assert path == join("id", "name~branch")

    path = conv.to_path(Target("name", "id", ("branch1", "branch2")))
    assert path == join("id", "name~branch1.branch2")

    path = conv.to_path(
        Target("so+me-na_me", ("A+B", "C-D", "E_F"), ("A+B", "C-D", "E_F"))
    )
    assert path == join("A+B", "C-D", "E_F", "so+me-na_me~A+B.C-D.E_F")

    with pytest.raises(ValueError):
        conv.to_path(Target("some.name"))

    # from path
    target = conv.from_path("_/name/")
    assert target == Target("name")

    target = conv.from_path(join("id", "name"))
    assert target == Target("name", "id")

    target = conv.from_path(join("id1", "id2", "name"))
    assert target == Target("name", ("id1", "id2"))

    target = conv.from_path(join("id1", "{id2.id3}", "name"))
    assert target == Target("name", ("id1", ("id2", "id3")))

    target = conv.from_path("_/name~branch")
    assert target == Target("name", None, "branch")

    target = conv.from_path(join("id", "name~branch"))
    assert target == Target("name", "id", "branch")

    target = conv.from_path(join("id", "name~branch1.branch2"))
    assert target == Target("name", "id", ("branch1", "branch2"))

    target = conv.from_path(join("A+B", "C-D", "E_F", "so+me-na_me~A+B.C-D.E_F"))
    assert target == Target("so+me-na_me", ("A+B", "C-D", "E_F"), ("A+B", "C-D", "E_F"))

    with pytest.raises(ValueError):
        conv.from_path(join("A", "B.C", "D"))

    # flat path
    conv2 = TargetToPath(sep_index=".", sep_main="|")
    path = conv2.to_path(Target("name", "id", ("branch1", "branch2")))
    assert path == "id|name~branch1.branch2"

    target = conv2.from_path("id1.id2|so+me-na_me~branch1.branch2")
    assert target == Target("so+me-na_me", ("id1", "id2"), ("branch1", "branch2"))


def test_target_converter_dedicated():
    """test alternative targer converter"""
    conv = TargetToPathDedicated("name")

    # with pytest.raises(NotImplementedError):
    #     conv.to_path(Target("name"))

    path = conv.to_path(Target("name", ("id1", "id2")))
    assert path == "id1.id2"

    path = conv.to_path(Target("name", None, ("br1", "br2")))
    assert path == "_~br1.br2"

    path = conv.to_path(Target("name", ("id1", "id2"), ("br1", "br2")))
    assert path == "id1.id2~br1.br2"

    path = conv.to_path(Target("name", ("A+B", "C-D", "E_F"), ("A+B", "C-D", "E_F")))
    assert path == "A+B.C-D.E_F~A+B.C-D.E_F"

    with pytest.raises(ValueError):
        conv.from_path("")

    target = conv.from_path("id")
    assert target == Target("name", "id")

    target = conv.from_path("id1.id2~br1")
    assert target == Target("name", ("id1", "id2"), "br1")

    target = conv.from_path("_~br1.br2")
    assert target == Target("name", None, ("br1", "br2"))

    target = conv.from_path(join("A+B.C-D.E_F~A+B.C-D.E_F"))
    assert target == Target("name", ("A+B", "C-D", "E_F"), ("A+B", "C-D", "E_F"))

    with pytest.raises(ValueError):
        conv.to_path(Target("other", "id"))

    # with branch
    conv = TargetToPathDedicated("name", branch="br1")

    path = conv.to_path(Target("name", ("id1", "id2"), "br1"))
    assert path == "id1.id2"
    assert conv.from_path("id1.id2") == Target("name", ("id1", "id2"), "br1")

    with pytest.raises(ValueError):
        conv.to_path(Target("name"))  # missing branch
    with pytest.raises(ValueError):
        conv.to_path(Target("name", branch="wrong"))  # wrong branch


#
# def test_target_converter_with_version(tmpdir):
#     """ test converting target to path """
#     tmpdir = pathlib.Path(tmpdir)
#
#     # target to path
#     conv = TargetToPathWithVersion(tmpdir, sep_index=".", sep_main="|")
#
#     path = conv.to_path(Target("name"))
#     assert path == "_|name_v1"
#
#     path = conv.to_path(Target("name", "id1", "br1", version=2))
#     assert path == "id1|name~br1_v2"
#
#     target = conv.from_path("_|name_v84")
#     assert target.version == 84
#
#     target = conv.from_path("id1|name~br1_v2")
#     assert target.version == 2
#
#     # use dates
#     versioner = VersionerDate()
#     conv = TargetToPathWithVersion(tmpdir, versioner=versioner, sep_main="|")
#
#     now = datetime.datetime.now()
#     path = conv.to_path(Target("name", version=now))
#     assert path == "_|name_v" + versioner.from_version(now)
#
#     target = conv.from_path("id1|name~br1_v" + versioner.from_version(now))
#     assert target.version == now


def test_id_path_expr():

    # fixed length id
    conv = IdToPathExpr("<id>/<id>.<id>", noid="_")
    assert conv.to_path(None) == "_"
    assert conv.to_path(("id1", "id2", "id3")) == "id1/id2.id3"
    with pytest.raises(ValueError):
        conv.to_path(("id1", "id2"))

    assert conv.from_path("_") is None
    assert conv.from_path("id1/id2.id3") == ("id1", "id2", "id3")
    with pytest.raises(ValueError):
        conv.from_path(".")
    with pytest.raises(ValueError):
        conv.from_path("id1.id2/id3")

    # generative
    conv = IdToPathExpr("<id>[.<id>]", noid="_")

    assert conv.to_path(None) == "_"
    assert conv.to_path(["id1"]) == "id1"
    assert conv.to_path(["id1", "id2", "id3"]) == "id1.id2.id3"

    # TODO
    # assert conv.to_path(["id1", ("id2", "id3"),]) == "id1.{id2.id3}"

    assert conv.from_path("_") is None
    assert conv.from_path("id1") == ("id1",)
    assert conv.from_path("id1.id2.id3") == ("id1", "id2", "id3")
    with pytest.raises(ValueError):
        conv.from_path("id1/id2")

    # generative with tail
    conv = IdToPathExpr("<id>[.<id>]/<id>", noid="_")
    assert conv.from_path("id1/id2") == ("id1", "id2")
    assert conv.from_path("id1.id2/id3") == ("id1", "id2", "id3")
    with pytest.raises(ValueError):
        conv.from_path("id1")
    with pytest.raises(ValueError):
        conv.from_path("id1.id2")
    with pytest.raises(ValueError):
        conv.from_path("id1/id2.id3")

    conv = IdToPathExpr("~[.<id>]<id>#")
    assert conv.prefix == "~"
    assert conv.suffix == "#"

    # with validation
    conv = IdToPathExpr(
        "<id1>[.<id2>]/<id3>", values={"id2": ["foo", "fee"], "id3": r"ba\w"}
    )
    assert conv.from_path("any.foo/bar") == ("any", "foo", "bar")
    assert conv.from_path("any.fee/baz") == ("any", "fee", "baz")
    assert conv.from_path("any/baz") == ("any", "baz")
    assert conv.from_path("any.foo.fee/baz") == ("any", "foo", "fee", "baz")
    with pytest.raises(ValueError):
        conv.from_path("any.wrong/bar")
    with pytest.raises(ValueError):
        conv.from_path("any.wrong/wrong")
    with pytest.raises(ValueError):
        conv.from_path("any/wrong")
    with pytest.raises(ValueError):
        conv.from_path("any.foo.wrong/baz")
    assert conv.from_path("any.wrong/wrong", validate=False) == (
        "any",
        "wrong",
        "wrong",
    )

    assert conv.to_path(("any", "foo", "bar")) == "any.foo/bar"
    assert conv.to_path(("any", "fee", "baz")) == "any.fee/baz"
    assert conv.to_path(("any", "baz")) == "any/baz"
    assert conv.to_path(("any", "foo", "fee", "baz")) == "any.foo.fee/baz"
    with pytest.raises(ValueError):
        conv.to_path(("any", "wrong", "bar"))
    with pytest.raises(ValueError):
        conv.to_path(("any", "foo", "wrong"))
    with pytest.raises(ValueError):
        conv.to_path(("any", "wrong"))
    with pytest.raises(ValueError):
        conv.to_path(("any", "foo", "wrong", "baz"))
    assert conv.to_path(("any", "wrong", "wrong"), validate=False) == "any.wrong/wrong"


def test_target_path_expr():

    # default converter
    conv = TargetToPathExpr()

    # to path
    assert conv._to_path(Target("name")) == "_/name"
    assert conv._to_path(Target("name", "id1")) == "id1/name"
    assert conv._to_path(Target("name", branch="br1")) == "_/name~br1"
    assert conv._to_path(Target("name", "id1", "br1")) == "id1/name~br1"
    assert (
        conv._to_path(Target("name", ("id1", "id2"), ("br1", "br2")))
        == "id1/id2/name~br1.br2"
    )

    with pytest.raises(ValueError):
        # invalid id
        conv._to_path(Target("name", "id1.id2", "br1"))

    # from path
    assert conv._from_path("_/name") == Target("name")
    assert conv._from_path("id1/name") == Target("name", "id1", None)
    assert conv._from_path("_/name~br1") == Target("name", branch="br1")
    assert conv._from_path("id1/name~br1") == Target("name", "id1", "br1")
    assert conv._from_path("id1/id2/name~br1.br2") == Target(
        "name", ("id1", "id2"), ("br1", "br2")
    )

    with pytest.raises(ValueError):
        conv._from_path("id1/name~")  # wrong branch

    with pytest.raises(ValueError):
        conv._from_path("id1.id2/name~br1")  # wrong id

    with pytest.raises(ValueError):
        conv._from_path("name~br1")  # wrong id

    # fixed length
    conv = TargetToPathExpr(index="<id>.<id>/<id>", branch="~<id>")
    assert (
        conv._to_path(Target("name", ("id1", "id2", "id3"), "br1"))
        == "id1.id2/id3/name~br1"
    )
    assert conv._from_path("id1.id2/id3/name~br1") == Target(
        "name", ("id1", "id2", "id3"), "br1"
    )

    with pytest.raises(ValueError):
        conv._to_path(Target("name", ("id1", "id2"), "br1"))
    with pytest.raises(ValueError):
        conv._to_path(Target("name", ("id1", "id2", "id3"), ("br1", "br2")))

    # dedicated target
    conv = TargetToPathExpr(index="<id>", branch="~<id>", name="name1")
    assert conv._to_path(Target("name1", "id1", "br1")) == "id1/name1~br1"
    with pytest.raises(ValueError):
        conv._to_path(Target("name2", "id1", "br1"))
    assert conv._from_path("id1/name1~br1") == Target("name1", "id1", "br1")

    # without name
    conv = TargetToPathExpr(
        struct="<index><branch>", index="<id>", branch="~<id>", name="name1"
    )
    assert conv._to_path(Target("name1", "id1", "br1")) == "id1~br1"
    with pytest.raises(ValueError):
        conv._to_path(Target("name2", "id1", "br1"))
    assert conv._from_path("id1~br1") == Target("name1", "id1", "br1")

    # default branch
    conv = TargetToPathExpr(default_branch="default")
    assert conv._to_path(Target("name", "id")) == "id/name"
    assert conv._to_path(Target("name", "id", "default")) == "id/name"
    assert conv._from_path("id/name") == Target("name", "id", "default")
    assert conv._from_path("id/name~any") == Target("name", "id", "default")
    with pytest.raises(ValueError):
        conv._to_path(Target("name", "id", "other"))

    # add branch
    # conv = TargetToPathExpr(
    #     struct="<index><branch>", index="<id>", branch="~<id>.new", name="name1"
    # )
    # assert conv._to_path(Target("name1", "id1", "br1")) == "id1~br1.new"
    # assert conv._from_path("id1~br1.new") = Target("name1", "id1", "br1")

    # with fixed branch
    # conv = TargetToPathExpr(
    #     struct="<index>/<name><branch>",
    #     index="<id>",
    #     branch="fixed",
    # )
    # assert conv._to_path(Target("name1", "id1", "fixed")) == "id1/name1~fixed"
    # assert conv._from_path("id1/name1~fixed") == Target("name1", "id1", "fixed")

    # with validation
    conv = TargetToPathExpr(
        index="<id1>[.<id2>]/<id3>",
        branch="~<br>[.<br>]",
        values={"id2": ["foo", "fee"], "id3": r"ba\w", "br": ["brA", "brB"]},
    )
    assert conv._to_path(Target("name", ("any", "bar"))) == "any/bar/name"
    assert conv._to_path(Target("name", ("any", "foo", "bar"))) == "any.foo/bar/name"
    assert (
        conv._to_path(Target("name", ("any", "foo", "fee", "bar")))
        == "any.foo.fee/bar/name"
    )
    assert (
        conv._to_path(Target("name", ("any", "foo", "bar"), "brA"))
        == "any.foo/bar/name~brA"
    )
    assert (
        conv._to_path(Target("name", ("any", "foo", "bar"), ("brA", "brB")))
        == "any.foo/bar/name~brA.brB"
    )
    with pytest.raises(ValueError):
        conv._to_path(Target("name", "any"))
    with pytest.raises(ValueError):
        conv._to_path(Target("name", ("any", "wrong")))
    with pytest.raises(ValueError):
        conv._to_path(Target("name", ("any", "foo", "wrong")))
    with pytest.raises(ValueError):
        conv._to_path(Target("name", ("any", "foo", "wrong", "bar")))
    with pytest.raises(ValueError):
        conv._to_path(Target("name", ("any", "foo", "bar"), "wrong"))
    with pytest.raises(ValueError):
        conv._to_path(Target("name", ("any", "foo", "bar"), ("brA", "wrong")))

    assert conv._from_path("any/bar/name") == Target("name", ("any", "bar"))
    assert conv._from_path("any.foo/bar/name") == Target("name", ("any", "foo", "bar"))
    assert conv._from_path("any.foo.fee/bar/name") == Target(
        "name", ("any", "foo", "fee", "bar")
    )
    assert conv._from_path("any.foo/bar/name~brA") == Target(
        "name", ("any", "foo", "bar"), "brA"
    )
    assert conv._from_path("any.foo/bar/name~brA.brB") == Target(
        "name", ("any", "foo", "bar"), ("brA", "brB")
    )
    with pytest.raises(ValueError):
        conv._from_path("any/name")
    with pytest.raises(ValueError):
        conv._from_path("any/wrong/name")
    with pytest.raises(ValueError):
        conv._from_path("any.foo/wrong/name")
    with pytest.raises(ValueError):
        conv._from_path("any.foo.wrong/bar/name")
    with pytest.raises(ValueError):
        conv._from_path("any.foo/bar/name~wrong")
    with pytest.raises(ValueError):
        conv._from_path("any.foo/bar/name~brA.wrong")

    # using _,+,- as separators
    conv = TargetToPathExpr(index="PREFIX_<id>[_<id>]")
    target = Target("name", ("id1+id2", "id3", "id4-(id5)"))
    path = conv.to_path(target)
    assert path == join("PREFIX_id1+id2_id3_id4-(id5)", "name")
    assert conv.from_path(path) == target
    with pytest.raises(ValueError):
        conv.to_path(Target("name", "id1_id2"))  # underscore in index is forbidden now
