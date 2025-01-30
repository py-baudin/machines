""" unittest for indexparser.py """

import yaml
import pathlib
import pytest
from machines import parsers, target, storages

Target = target.Target
Id = target.Identifier
Index = target.Index
Branch = target.Branch
Storage = storages.TargetStorage
IndexParser = parsers.IndexParser
IndexParserError = parsers.IndexParserError
parse_batch = parsers.parse_batch
BatchFileError = parsers.BatchFileError


def test_parse_identifiers():

    # dummy targets
    storage = Storage()
    storage.write(Target("A", "id1"), None)
    storage.write(Target("B", "id2"), None)
    storage.write(Target("A", "id1", "br1"), None)
    storage.write(Target("A", ("id1", "id2"), ("br1", "br2")), None)
    storage.write(Target("C", branch="br2"), None)

    # new parser
    parser = IndexParser(storage)

    # no searching (ids don't have to exist)
    assert parser.parse_identifiers("id") == [Id("id", None)]
    assert parser.parse_identifiers("'id'") == [Id("id", None)]
    assert parser.parse_identifiers("id~") == [Id("id", None)]
    assert parser.parse_identifiers("id1.id2") == [Id(("id1", "id2"), None)]
    assert parser.parse_identifiers("~") == [Id(None, None)]
    assert parser.parse_identifiers("~br2") == [Id(None, "br2")]
    assert parser.parse_identifiers("id1~[br1|br2]") == [
        Id("id1", "br1"),
        Id("id1", "br2"),
    ]
    assert parser.parse_identifiers(["id1", "id2~br2"]) == [
        Id("id1", None),
        Id("id2", "br2"),
    ]

    with pytest.raises(IndexParserError):
        parser.parse_identifiers("id*", search=False)

    # with wildcards
    assert parser.parse_identifiers("wrong*") == []
    assert set(parser.parse_identifiers("id*~")) == {Id("id1", None), Id("id2", None)}
    assert set(parser.parse_identifiers("id1~*")) == {Id("id1", None), Id("id1", "br1")}
    assert set(parser.parse_identifiers("id1*")) == {
        Id("id1", None),
        Id("id1", "br1"),
        Id(("id1", "id2"), ("br1", "br2")),
    }
    assert set(parser.parse_identifiers(".")) == {
        Id("id1", None),
        Id("id2", None),
        Id("id1", "br1"),
        Id(("id1", "id2"), ("br1", "br2")),
    }  # (ignore no-index ids)
    assert parser.parse_identifiers("~*") == [Id(None, "br2")]

    # groups
    assert set(parser.parse_identifiers("[id1|id2]*~")) == {
        Id("id1", None),
        Id("id2", None),
    }
    assert set(parser.parse_identifiers("*~[br1|br1.br2]")) == {
        Id("id1", "br1"),
        Id(("id1", "id2"), ("br1", "br2")),
    }

    # some
    assert set(parser.parse_identifiers("*~$")) == {
        Id("id1", "br1"),
        Id(("id1", "id2"), ("br1", "br2")),
    }  # (ignore no-index ids)


def test_parse_targets():

    # dummy targets
    storage = Storage()
    storage.write(Target("A", "id1"), None)
    storage.write(Target("B", "id2"), None)
    storage.write(Target("A", "id1", "br1"), None)
    storage.write(Target("A", ("id1", "id2"), ("br1", "br2")), None)
    storage.write(Target("C", branch="br2"), None)

    # new parser
    parser = IndexParser(storage)

    with pytest.raises(IndexParserError):
        parser.parse_targets("id", exists=False)  # no target name

    # no searching (ids don't have to exist)
    assert parser.parse_targets("id#A", exists=False) == [Target("A", "id", None)]
    assert parser.parse_targets("#C", exists=False) == [Target("C", None, None)]

    # with exist
    assert parser.parse_targets("id#A") == []
    assert parser.parse_targets("id1#A") == [Target("A", "id1", None)]
    assert parser.parse_targets("#C~br2") == [Target("C", None, "br2")]
    assert parser.parse_targets("#C*") == [Target("C", None, "br2")]
    assert parser.parse_targets("#*") == [Target("C", None, "br2")]
    assert parser.parse_targets("#C~") == []

    # with wildcards
    assert parser.parse_targets("wrong*") == []
    assert set(parser.parse_targets("id*~")) == {Target("A", "id1"), Target("B", "id2")}
    assert set(parser.parse_targets("[id1|id2]*~")) == {
        Target("A", "id1"),
        Target("B", "id2"),
    }
    assert set(parser.parse_targets("id*#A~")) == {Target("A", "id1")}
    assert set(parser.parse_targets("*#A")) == {
        Target("A", "id1"),
        Target("A", "id1", "br1"),
        Target("A", ("id1", "id2"), ("br1", "br2")),
    }
    assert set(parser.parse_targets("*id2")) == {
        Target("B", "id2"),
        Target("A", ("id1", "id2"), ("br1", "br2")),
    }
    assert set(parser.parse_targets("*~br1")) == {Target("A", "id1", "br1")}
    assert set(parser.parse_targets("*~[br1|br2]")) == {
        Target("A", "id1", "br1"),
        Target("C", None, "br2"),
    }
    assert set(parser.parse_targets(".")) == {
        Target("A", "id1", None),
        Target("B", "id2", None),
        Target("A", "id1", "br1"),
        Target("A", ("id1", "id2"), ("br1", "br2")),
        Target("C", None, "br2"),
    }
    assert set(parser.parse_targets("*~$")) == {
        Target("A", "id1", "br1"),
        Target("A", ("id1", "id2"), ("br1", "br2")),
        Target("C", None, "br2"),
    }
    assert set(parser.parse_targets("#*")) == {Target("C", None, "br2")}


def test_parse_batch():

    # dummy targets
    storage = Storage()
    storage.write(Target("A", "id1"), None)
    storage.write(Target("B", "id2"), None)
    storage.write(Target("A", "id1", "br1"), None)
    storage.write(Target("A", ("id1", "id2"), ("br1", "br2")), None)
    storage.write(Target("C", branch="br2"), None)

    # new parser
    parser = IndexParser(storage)

    # programs
    programs = {"prog1": ["prog1"], "prog2": ["prog2"]}

    # batch file
    batch = yaml.safe_load(
        """
    !task task1:
        inputs: id1
        param1: value1
        !program prog1:
            param2: value2
        !target A:
            foo: bar
    """
    )

    tasks, attachments = parse_batch(batch, parser, programs)
    assert len(tasks) == 1
    assert tasks[0]["program"] == "prog1"
    assert tasks[0]["task"] == "task1"
    assert tasks[0]["input_indices"] == ["id1"]
    assert tasks[0]["output_indices"] == None  # implicit
    assert tasks[0]["input_branches"] == [Branch(None)]  # no branch
    assert tasks[0]["output_branches"] == None  # implicit
    assert tasks[0]["parameters"] == {"param1": "value1", "param2": "value2"}
    assert tasks[0]["identifiers"] == [Id("id1", None)]
    assert attachments[Target("A", "id1")] == {"foo": "bar"}

    # complicated batch file
    batch = yaml.safe_load(
        """
    CONFIG: #no config
    !task task1:
        inputs: "[id1|id2]*~"
        outputs: [ID1~BR1, ID2~BR2]
        param1: value1
        !program prog1:
            param2: value2
        !target A:
            foo: bar
    """
    )

    tasks, attachments = parse_batch(batch, parser, programs)
    assert len(tasks) == 1
    assert tasks[0]["program"] == "prog1"
    assert tasks[0]["task"] == "task1"
    assert tasks[0]["input_indices"] == ["id1", "id2"]
    assert tasks[0]["output_indices"] == ["ID1", "ID2"]  # explicit
    assert tasks[0]["input_branches"] == [Branch(None), Branch(None)]  # no branch
    assert tasks[0]["output_branches"] == [Branch("BR1"), Branch("BR2")]
    assert tasks[0]["parameters"] == {"param1": "value1", "param2": "value2"}
    # using output ids
    assert tasks[0]["identifiers"] == [Id("ID1", "BR1"), Id("ID2", "BR2")]
    assert attachments[Target("A", "ID1", "BR1")] == {"foo": "bar"}

    #
    # legacy batch file
    batch = yaml.safe_load(
        r"""
    CONFIG:
        ALIAS: {alias: [prog1, prog2]}
        PARAMETERS:
            prog2: {param3: value3}
        PREFIX: prefix

    id1~br1:
        path: "some/path"
        alias:
            param2: value2
    """
    )

    tasks, attachments = parse_batch(
        batch,
        parser,
        programs,
        new_branches=("br2", "br3"),
        check_path=False,
    )
    assert len(tasks) == 2
    assert tasks[0]["program"] == "prog1"
    assert tasks[0]["task"] == "id1~br1"
    assert tasks[0]["input_indices"] == ["id1"]
    assert tasks[0]["output_indices"] == None  # implicit
    assert tasks[0]["input_branches"] == [Branch("br1")]
    assert tasks[0]["output_branches"] == Branch("br2", "br3")
    assert tasks[0]["parameters"] == {
        "path": str(pathlib.Path("prefix").absolute() / "some" / "path"),
        "param2": "value2",
    }
    # using input ids
    assert tasks[0]["identifiers"] == [Id("id1", "br1")]

    assert tasks[1]["program"] == "prog2"
    assert tasks[1]["task"] == "id1~br1"
    assert tasks[1]["parameters"] == {
        "path": str(pathlib.Path("prefix").absolute() / "some" / "path"),
        "param2": "value2",
        "param3": "value3",
    }

    # invalid batch
    batch = yaml.safe_load(
        """
    !task task1:
        param1: value1
        !program prog1:
            param2: value2
    """
    )
    with pytest.raises(BatchFileError):
        parse_batch(batch, parser, programs)

    batch = yaml.safe_load(
        """
    !task task1:
        inputs: id1
        path: null
        !program prog1:
            param2: value2
    """
    )
    with pytest.raises(BatchFileError):
        ans = parse_batch(batch, parser, programs)


def test_parse_batch_2():
    """advanced batch files"""

    # dummy targets
    storage = Storage()
    storage.write(Target("A", "id1"), None)
    storage.write(Target("B", "id2"), None)
    storage.write(Target("A", "id1", "br1"), None)
    storage.write(Target("A", ("id1", "id2"), ("br1", "br2")), None)
    storage.write(Target("C", branch="br2"), None)

    # new parser
    parser = IndexParser(storage)

    # programs
    programs = {"prog1": ["prog1"], "prog2": ["prog2"]}

    # using !macro tags
    batch = yaml.safe_load(
        """
    CONFIG:
        PARAMETERS:
            # macro instruction
            prog2: {!macro COPY: prog1}

    !task task1:
        inputs: id1
        !program prog1:
            param1: A
            param2: B
        !program prog2:
            param1: C
    """
    )
    tasks, _ = parse_batch(batch, parser, programs)
    assert len(tasks) == 2
    assert tasks[0]["program"] == "prog1"
    assert tasks[0]["parameters"] == {"param1": "A", "param2": "B"}
    assert tasks[1]["program"] == "prog2"
    assert tasks[1]["parameters"] == {"param1": "C", "param2": "B"}

    # using !macro condition
    batch = yaml.safe_load(
        """
    CONFIG:
        PARAMETERS:
            # macro instruction
            prog1: {!macro CONDITION: param1 > 0}

    !task task1:
        inputs: id1
        !program prog1:
            param1: -1

    !task task2:
        inputs: id2
        !program prog1:
            param1: 1
    """
    )
    tasks, _ = parse_batch(batch, parser, programs)
    assert len(tasks) == 1
    assert tasks[0]["task"] == "task2"


def test_batch_templates():
    template = {
        "task1~B<param1>.B<param2>": {
            "param1": "<param1>",
            "param2": "<param2>",
        }
    }

    # simple
    cases = {"param1": 1, "param2": 2}
    batch = parsers.generate_template(template, cases)
    assert batch == {"task1~B1.B2": {"param1": 1, "param2": 2}}

    # sum
    cases = [{"param1": 1, "param2": 2}, {"param1": 3, "param2": 4}]
    batch = parsers.generate_template(template, cases)
    assert batch == {
        "task1~B1.B2": {"param1": 1, "param2": 2},
        "task1~B3.B4": {"param1": 3, "param2": 4},
    }

    # product
    cases = [{"param1": [1, 2]}, {"param2": [3, 4]}]
    batch = parsers.generate_template(template, cases)
    assert batch == {
        "task1~B1.B3": {"param1": 1, "param2": 3},
        "task1~B1.B4": {"param1": 1, "param2": 4},
        "task1~B2.B3": {"param1": 2, "param2": 3},
        "task1~B2.B4": {"param1": 2, "param2": 4},
    }

    # more complicated, using list/dict
    template = {"task~<branch>": {"param1": "<param1>", "param2": "<param2>"}}
    cases = [
        {"param1": [[1, 2]]},
        {"param2": [{3: 4}, {5: 6}], "branch": ["complex1", "complex2"]},
    ]
    batch = parsers.generate_template(template, cases)
    assert batch == {
        "task~complex1": {"param1": [1, 2], "param2": {3: 4}},
        "task~complex2": {"param1": [1, 2], "param2": {5: 6}},
    }
