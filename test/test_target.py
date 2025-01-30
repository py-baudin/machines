# -*- coding: utf-8 -*-
""" test targets and identifiers """
import json
import pytest
from machines.target import Target, IdBase, Index, Branch, ravel_identifiers


def test_identifier():

    # empty id
    id = IdBase()

    assert id == IdBase()
    assert id == IdBase(None)
    assert id == IdBase("")
    assert id == IdBase(" ")
    assert not bool(id)
    assert not (id is None)

    # simple id
    id = IdBase(0)

    assert id == "0"  # ints are converted to str
    assert id == IdBase("0")
    assert bool(id)

    with pytest.raises(TypeError):
        # float
        IdBase(1.0)

    # simple id 2
    id = IdBase("foobar")
    assert id == "foobar"
    assert id == IdBase("foobar")
    assert id == IdBase("  foobar")

    # other values
    assert "foo+bar" == IdBase("foo+bar")
    assert "foo_bar" == IdBase("foo_bar")
    assert "foo-bar" == IdBase("foo-bar")

    # multi id
    id = IdBase("foo", "bar")
    assert id == IdBase("foo", "bar")
    assert id == IdBase(("foo", "bar"))
    assert id == ("foo", "bar")

    # int vs str
    id = IdBase("1", 2)
    assert id == IdBase(1, "2") == IdBase(1, 2)

    # concatenate
    id = IdBase("foo") + IdBase("bar")
    assert id == ("foo", "bar")
    assert id == "foo" + IdBase("bar")
    assert id == IdBase("foo") + "bar"

    # sub-id
    id = IdBase("foo", ("bar", "baz"))
    assert id == ("foo", ("bar", "baz"))
    assert id == "foo" + IdBase((("bar", "baz"),))
    assert id == IdBase("foo") + (("bar", "baz"),)

    assert id[0] == "foo"
    assert id[1] == ("bar", "baz")
    assert "foo" in id
    assert ("bar", "baz") in id

    assert IdBase("foobar")
    assert not IdBase()
    assert not IdBase() is None
    assert IdBase("foobar").values == "foobar"
    assert IdBase().values is None
    assert IdBase("foo", "bar").values == ("foo", "bar")

    # duplicates
    assert IdBase("a") == IdBase("a", "a")

    id1 = IdBase("id1")
    id12 = id1 + "id2"
    assert id12 == IdBase("id1", "id2")

    # type error

    with pytest.raises(TypeError):
        # list
        IdBase([1, 2])

    with pytest.raises(TypeError):
        # float
        IdBase((1, 1.2))

    with pytest.raises(TypeError):
        # float
        IdBase(1, 1.2)

    with pytest.raises(TypeError):
        # float
        IdBase("foobar", (1, 1.2))

    # value error

    with pytest.raises(ValueError):
        IdBase(None, 1)

    with pytest.raises(ValueError):
        IdBase(1, (None, 2))

    # id match
    assert IdBase(None).match(None)
    assert IdBase(None).match("")
    assert IdBase(None).match("*")  # special case
    assert not IdBase(None).match("*", match_null=False)  # special case
    assert not IdBase(None).match("foobar")
    assert IdBase("foobar").match("foobar")
    assert not IdBase("foobar").match("foobaz")
    assert IdBase("foobar").match("*")
    assert IdBase("foobar").match("*bar")
    assert IdBase("foobar").match("foo*")
    assert not IdBase("foobar").match("*baz")
    assert IdBase(("foo", "bar")).match(("foo", "bar"))
    assert IdBase(("foo", "bar")).match(("foo", "b*r"))
    assert not IdBase(("foo", "bar")).match(("foo", "b*z"))

    # crop
    id = IdBase("a", "b", "c")
    assert id.crop(1) == IdBase("a", "b")
    assert id.crop(2) == IdBase("a")
    assert id.crop(3) == IdBase()
    assert id.crop(4) is None

    # comparisons
    assert IdBase("a") == IdBase("a")
    assert IdBase("a") != IdBase("b")
    assert IdBase("a") < IdBase("b")
    assert IdBase("b") > IdBase("a")
    assert IdBase("a") <= IdBase("a")

    assert IdBase(None) == IdBase(None)
    assert IdBase(None) <= IdBase(None)
    assert IdBase("a") != IdBase(None)
    assert IdBase("a") < IdBase(None)
    assert IdBase(None) > IdBase("a")

    assert IdBase("a") < IdBase(("a", "b"))
    assert IdBase("b") > IdBase(("a", "b"))
    assert IdBase("a", "b") > IdBase("a")

    assert IdBase(("a", "b")) == IdBase(("a", "b"))
    assert IdBase(("a", "b")) != IdBase(("a", "c"))
    assert IdBase(("a", "b")) < IdBase(("a", "c"))
    assert IdBase(("a", "b")) < IdBase(("b", "a"))
    assert IdBase(("a", "b")) < IdBase(("a", "c", "a"))
    assert IdBase(("a", "b")) < IdBase(None)


def test_id_branch():

    id = Index("foobar")
    branch = Branch("foobar")

    with pytest.raises(TypeError):
        id + branch

    # id allows duplicates
    id1 = Index("id1", "id2")
    assert id1 + "id1" != Index("id1", "id2")
    assert id1 + "id1" == Index("id1", "id2", "id1")

    # branch does not
    br1 = Branch("br1", "br2")
    assert br1 + "br1" == Branch("br1", "br2")

    # branch add: remove duplicates
    assert Branch("br1", "br2") + ("br1", "br3") == Branch("br1", "br2", "br3")


def test_target_class():

    target = Target("name")
    assert target == Target("name", None, None)

    # other values
    assert Target("foo_bar").name == "foo_bar"
    assert Target("foo+bar").name == "foo+bar"
    assert Target("foo-bar").name == "foo-bar"
    with pytest.raises(TypeError):
        Target(1)  # wrong type
    with pytest.raises(ValueError):
        Target("foo bar")  # wrong value

    target = Target("name", "foo")
    assert target == Target("name", "foo", None)

    target = Target("name", branch="foo")
    assert target == Target("name", None, "foo")

    target = Target("name", "foo", "bar")
    assert target == Target("name", "foo", "bar")

    target = Target("name", Index("some", "id"), Branch("some", "branch"))
    assert target == Target("name", Index("some", "id"), Branch("some", "branch"))

    with pytest.raises(TypeError):
        Target(1, None, None)

    with pytest.raises(TypeError):
        Target(None, None, None)

    # duplicate values
    target = Target("name", Index("foo", "foo"))
    assert target == Target("name", Index("foo", "foo"))

    # no duplicates in branches
    target = Target("name", branch=Branch("foo", "foo"))
    assert target == Target("name", branch="foo")

    # test attachment
    target = Target("name")
    target.attach({"some": "info"})
    with pytest.raises(ValueError):
        # already set
        target.attach({"some": "other info"})
    assert target.attachment == {"some": "info"}

    # test update
    target = Target("name", "id", "br")
    target2 = target.update(name="name2")
    assert target2.name == "name2"
    assert target2.identifier == ("id", "br")
    assert target2.type == None

    target2 = target.update(branch=None)
    assert target2.index == Index("id")
    assert target2.branch == Branch(None)

    target2 = target.update(index=None, type="test")
    assert target2.index == Index(None)
    assert target2.branch == Branch("br")
    assert target2.type == "test"

    # test serialize
    target3 = Target("name", ("id1", ("id2", "id3")), ("br1", ("br2", "br3")))
    serialized = json.dumps(target3.serialize())
    assert Target.deserialize(**json.loads(serialized)) == target3

    # test match
    assert Target("foobar").match("foobar")
    assert Target("foobar").match("foo*")
    assert Target("foobar").match("*bar")
    assert Target("foobar").match("*")

    assert not Target("foobar").match("foobaz")
    assert not Target("foobar").match("*baz")
    assert not Target("foobar").match("")

    assert Target("foobar", "id1").match("foobar", "id1")
    assert Target("foobar", "id1", "br1").match("foo*", "id1", "br1")
    assert Target("foobar", "id1", "br1").match("foo*", "id1", "br*")
    assert not Target("foobar", "id1", "br1").match("foo*", "id2", "br*")

    # comparisons
    assert Target("name") == Target("name")
    assert Target("name") != Target("name", "id")
    assert Target("name") > Target("name", "id")

    assert Target("name") != Target("name", branch="br")
    assert Target("name") < Target("name", branch="br")

    assert Target("name", "id") < Target("name", "id", "br")
    assert Target("name", "id") < Target("name", branch="br")

    assert Target("name", "id1") < Target("name", ("id1", "id2"))
    assert Target("name", "id2") > Target("name", ("id1", "id2"))

    assert Target("A", "id1") < Target("B", "id1")
    assert Target("A", "id2") > Target("B", "id1")
    assert Target("A", "id1", "br1") < Target("A", "id1", "br2")
    assert Target("B", "id1", "br1") > Target("A", "id1", "br2")


def test_ravel_indices():

    indices = ravel_identifiers()
    assert indices == [(None, None)]

    indices = ravel_identifiers("id1")
    assert indices == [("id1", None)]

    indices = ravel_identifiers(branches="br1")
    assert indices == [(None, "br1")]

    indices = ravel_identifiers("id1", "br1")
    assert indices == [("id1", "br1")]

    indices = ravel_identifiers(["id1", "id2"], "br1")
    assert indices == [("id1", "br1"), ("id2", "br1")]

    indices = ravel_identifiers("id1", ["br1", "br2"])
    assert indices == [("id1", "br1"), ("id1", "br2")]
