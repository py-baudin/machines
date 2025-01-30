""" Index and batch file parsers

## Identifier and target syntax

Identifiers syntax:
    # fully defined (no searching)
    id, id~, id~branch, id1.id2~br1.br2,

    # partially defined (using wildcards)
    # wildcars are: '*' (incl. none) and '$' (at least one)
    . (all), id*, id*~, *id, id1*id2, id~br*, *~br, id~*

    # alternate values
    [id1|id2], id1.[id2a|id2b]*, id~[br1|br2]

    # no-id or no-branch
    _~br, id~


Targets syntax:
    # fully defined (no searching)
    id#name~, id#name~branch

    # partially defined (using wildcards)
    *#name~, id#*~, id#name~*


## Batch file formating
A YAML batch file format is a list of tasks, with special keys:

```yaml
!task task1:
    # identifiers (at least one of input or outputs must be set)
    [inputs: <ids>]
    [outputs: <ids>]

    !program prog1:
        # set program parameters
        param1: value1
        path: path1
        ...

    !target target-name:
        # set attachement to output targets
        obj1: ...

    # common parameters
    param2: value
    path: <path>
```

Notes:

- 'path' is a special parameter: if the provided path is not absolute,
the local path of the batch file is automatically prepended to it.


### legacy/simple format

Alternatively, one can use a simpler batch form, where the task's name
is the outpout identifier and the "!program" keys can be omitted.

When one needs different values for the input and output ids,
or process multiple targets at once, it is preferable to use the full-form
batch syntax defined above.

```yaml
<id>:
    program-name:
        param1: ...
```

### Configuration "header"

A special structure can be put (preferentially at the top of the file) to
provide program aliases, (program-)global parameters and path options.

```yaml
CONFIG:
    ALIAS:
        # program aliases: 'alias' will be substituded
        # for all the program names defined in values.
        alias: [prog1, prog2]
    PARAMETERS:
        # global parameter values
        param1: value1
        prog1:
            param2: value2
    PATH:
        # path options
        PREFIX: <path-prefix>
```


### Macro instructions


```yaml
CONFIG:
  PARAMETERS:
    prog2:
      !macro COPY: prog1 # copy values from prog1
      !macro CONDITION: param2 > 0 # run only if condition is met

task1:
    inputs: ...
    prog1:
        param1: value1
    prog2:
        param2: value2
```

"""

import os
import warnings
import pathlib
import yaml
import re
import itertools
from .target import Target, Identifier, Index, Branch
from .utils import id_from_string, id_to_string, target_repr, identifier_repr


class IndexParserError(ValueError):
    """Exception class for parsing errors"""

    pass


class BatchFileError(Exception):
    """Exception for handling batch files parsing errors"""


class IndexParser:
    """Parse identifiers and targets

    Usage:
        parser = IndexParser(storages)

        # return list of matching identifiers
        parser.identifiers(string)

        # return list of matching targets
        parser.targets(string)

    """

    def __init__(self, *storages):  # targets
        """Init IndexParser

        storages: TargetStorage objects
        """

        # list targets in storages
        targets = {target for storage in storages for target in storage.list()}
        identifiers = set(target.identifier for target in targets)

        self.primary = "#"
        self.secondary = "~"
        self.sepindex = "."
        self.sepbranch = "."

        self.groupdel = "[]"
        self.groupsep = "|"

        self.wc_all = "."
        self.wc_any = "*"
        self.wc_some = "$"

        self.strids = {}
        self.strtargets = {}
        self.update_identifiers(identifiers)
        self.update_targets(targets)

    def update_identifiers(self, identifiers):
        strids = {
            Identifier(index, branch): identifier_repr(
                index, branch, sep2=self.secondary
            )
            for index, branch in identifiers
        }
        self.strids = {**strids, **self.strids}

    def update_targets(self, targets):
        strtargets = {
            target: target_repr(
                target.name,
                target.index,
                target.branch,
                sep1=self.primary,
                sep2=self.secondary,
                nobranch=self.secondary,
            )
            for target in targets
        }
        self.strtargets = {**strtargets, **self.strtargets}

    def identifiers(self, strids, callback, search=True, exit=True):
        """parse and return list of identifiers with callback on error and optional exit"""
        try:
            return self.parse_identifiers(strids, search=search)
        except IndexParserError as exc:
            callback(exc)
        if exit:
            raise SystemExit()

    def targets(self, strids, callback, search=True, exists=True, exit=True):
        """parse and return list of targets with callback on error and optional exit"""
        try:
            return self.parse_targets(strids, search=search, exists=exists)
        except IndexParserError as exc:
            callback(exc)
        if exit:
            raise SystemExit()

    def parse_identifiers(self, strids, search=True):
        """parse identifier expression and return list of identifiers"""
        grouped = self._parse_groups(strids)
        try:
            return [
                match
                for string in grouped
                for match in self._parse_identifier_expr(string, search)
            ]
        except ValueError as exc:
            raise IndexParserError(exc)

    def parse_targets(self, strids, search=True, exists=True):
        """parse target expression and return list of targets"""
        grouped = self._parse_groups(strids)
        try:
            return [
                match
                for string in grouped
                for match in self._parse_target_expr(string, search, exists)
            ]
        except ValueError as exc:
            raise IndexParserError(exc)

    def indices(self, strids):
        """parse index from string expression"""
        return id_from_string(strids, self.sepindex)

    def branches(self, strids):
        """parse branch from string expression"""
        return id_from_string(strids, self.sepbranch, none="")

    def _parse_groups(self, strids):
        """parse string ids for groups"""
        if isinstance(strids, (list, tuple)):
            return [group for item in strids for group in self._parse_groups(item)]
        elif not set(self.groupdel) & set(strids):
            return [strids]

        groups = {}

        def _replace(match):
            matchstr = match.group(1)
            group = f"__group_{len(groups)}"
            values = matchstr[1:-1].split(self.groupsep)
            if len(values) <= 1:
                raise ValueError(f"Invalid group syntax: {matchstr}")
            groups[group] = values
            return f"{{{group}}}"

        # replace groups
        replaced = re.sub(r"(\[[^\]]+\])", _replace, strids)

        # return all combinations
        versions = []
        for comb in itertools.product(*list(groups.values())):
            format = {group: value for group, value in zip(groups, comb)}
            versions.append(replaced.format(**format))

        return versions

    def _parse_target_expr(self, string, search, exists):
        """parse string target and return list of targets"""
        string = string.strip("' ")
        if string == self.wc_all:
            # return all targets
            return sorted(self.strtargets)

        elif not {self.wc_any, self.wc_some} & set(string):
            # no searching
            if not self.primary in string:
                # invalid id
                raise IndexParserError(f"Missing target's name: {string}")

            index, tail = string.split(self.primary)
            split = tail.split(self.secondary)
            if len(split) == 1:
                name = split[0]
                branch = ""
            else:
                name = split[0]
                branch = split[1]

            index = id_from_string(index, self.sepindex, none="")
            branch = id_from_string(branch, self.sepbranch, none="")

            targets = [Target(name, index, branch)]
            if not exists:
                return targets
            # only return existing targets
            return [target for target in targets if target in self.strtargets]

        elif not search:
            raise IndexParserError("Wildcards are not accepted ")

        # else: search targets

        # start char
        startchar = "^"  # if string[0] != self.secondary else ""
        # end character
        endchar = "$" if self.secondary in string else ""

        # regex
        string = self._escape_string(string)
        regex = re.compile(startchar + string + endchar)
        match = [
            target
            for target, strtarget in self.strtargets.items()
            if regex.search(strtarget)
        ]

        return match

    def _parse_identifier_expr(self, string, search):
        """parse target's identifier (index + branch) from string"""
        string = string.strip("' ")
        if string == self.wc_all:
            # return all targets except those with index=None
            return [id for id in self.strids if id.index]

        elif self.primary in string:
            # invalid id
            raise IndexParserError(f"Cannot have '{self.primary}' in identifier string")

        elif not {self.wc_any, self.wc_some} & set(string):
            split = string.split(self.secondary)
            if len(split) == 1:
                head = split[0]
                tail = ""
            else:
                head = split[0]
                tail = split[1]

            index = id_from_string(head, self.sepindex, none="")
            branch = id_from_string(tail, self.sepbranch, none="")
            return [Identifier(index, branch)]
        elif not search:
            raise IndexParserError("Wildcards are not accepted ")

        # else: search identifiers

        # is all indices ?
        not_all_indices = string.split("~")[0] != "*"

        # start char
        startchar = "^"  # if string[0] != self.secondary else ""
        # end character
        endchar = "$" if self.secondary in string else ""

        # regex
        string = self._escape_string(string)
        regex = re.compile(startchar + string + endchar)
        match = [
            id
            for id, strid in self.strids.items()
            if (regex.search(strid) and (not_all_indices or id.index))
        ]

        return match

    def _escape_string(self, string, chars="."):
        # remove excape chars
        string = string.replace("^*", "*")
        # has wildcard
        if self.wc_any in string:
            string = string.replace(self.wc_any, "__WILDCARD_0__")
        if self.wc_some in string:
            string = string.replace(self.wc_some, "__WILDCARD_1__")
        # escape
        string = (
            re.escape(string)
            .replace("__WILDCARD_0__", f"{chars}*")
            .replace("__WILDCARD_1__", f"{chars}+")
        )
        return string


def parse_batch(file, indexparser, programs=[], new_branches=None, check_path=True):
    """parse YAML batch file

    Arguments:
        file: batchfile filename or dictionary
        indexparser: IndexParser object
        programs: dictionary of program aliases ({alias1: [prog1, prog2], ...})
        new_branches: additional branch(es) for all output identifiers

    """

    def load_batch(filename):
        # load YAML
        if not filename.is_file():
            raise BatchFileError(f"Could not find batch file: {filename}")
        with open(filename) as fp:
            try:
                batch = yaml.safe_load(fp)
            except Exception as exc:
                raise BatchFileError(f"Invalid batch file ({filename}): {exc}")
            if not batch:
                raise BatchFileError(f"Empty batch file ({filename})")
        return batch

    if isinstance(file, dict):
        # a dictionary
        batch = file
        filename = pathlib.Path(".")
    else:
        # a path
        filename = pathlib.Path(file)
        batch = load_batch(filename)

    # append branches
    if new_branches is not None:
        newbranch = Branch(new_branches)
    else:
        newbranch = None

    # batch config
    config = batch.pop("CONFIG", {})
    if not config:
        config = {}

    if "INCLUDE" in config:
        # include other batch files (must be in same directory)
        include = config.pop("INCLUDE")
        include = include if isinstance(include, list) else [include]
        root = filename.parent
        for file in include:
            if not (root / file).is_file():
                raise BatchFileError(f"Unknown include batch: {root / file}")
            other = load_batch(root / file)
            other.pop("CONFIG", None)  # remove existing config
            batch = {**other, **batch}

    # preset programs (update with config)
    preset_programs = {**programs}
    for name in config.get("ALIAS", {}):
        new_aliases = config["ALIAS"][name]
        if isinstance(new_aliases, str):
            new_aliases = [new_aliases]
        existing_aliases = preset_programs.get(name, [])
        preset_programs[name] = sorted(set(new_aliases) | set(existing_aliases))

    # tasks modifiers
    modifiers = config.get("TASKS", {})
    # update preset_programs
    proxy_programs = {
        name: [name]
        for task in modifiers.values()
        for name in task
        if isinstance(task[name], dict) and "program" in task[name]
    }
    preset_programs.update(proxy_programs)

    # global parameters
    global_params = config.get("PARAMETERS", {})

    # templates
    if "GENERATE" in config:
        batch = generate_template(batch, config["GENERATE"])

    # path options
    try:
        path_prefix = config.get("PATH", config.get("PREFIX", "."))
        path_prefix = pathlib.Path(path_prefix.replace("\\", os.path.sep).replace("/", os.path.sep))
        if path_prefix.is_absolute():
            path_prefix = path_prefix.resolve()
        else:
            path_prefix = (pathlib.Path(filename.parent) / path_prefix).resolve()
    except:
        raise BatchFileError(f"Invalid path prefix: {config.get('PATH')}")
    
    # parse batch
    tasks = []
    attachments = {}
    for taskname, task in batch.items():
        # parse i/o ids
        if not task:
            task = {}
        if {"inputs", "outputs"} <= set(task):
            # both inputs and outputs
            input_ids = indexparser.parse_identifiers(task.pop("inputs"))
            output_ids = indexparser.parse_identifiers(task.pop("outputs"))
        elif "inputs" in task:
            # only inputs: automatic output
            input_ids = indexparser.parse_identifiers(task.pop("inputs"))
            output_ids = None
        elif "outputs" in task:
            # only outputs: inputs are the same
            output_ids = indexparser.parse_identifiers(task.pop("outputs"))
            input_ids = output_ids
        elif isinstance(taskname, str):
            # taskname is the input_ids
            input_ids = indexparser.parse_identifiers(taskname)
            output_ids = None
        else:
            raise BatchFileError(
                f"Invalid batch task '{taskname}': no 'inputs' and/or 'outputs' identifiers"
            )

        # swap task obj for string
        taskname = taskname.name if isinstance(taskname, YAMLTask) else taskname

        if output_ids:
            task_ids = output_ids
        else:
            task_ids = input_ids

        # output indices and branches
        input_indices = [id.index for id in input_ids]
        output_indices = None if not output_ids else [id.index for id in output_ids]
        input_branches = [Branch(id.branch) for id in input_ids]
        output_branches = (
            newbranch
            if not output_ids
            else [Branch(id.branch) + newbranch for id in output_ids]
        )

        # task's programs
        programs = {
            p.name: task.pop(p) for p in list(task) if isinstance(p, YAMLProgram)
        }

        # output target's attachement
        targets = {p.name: task.pop(p) for p in list(task) if isinstance(p, YAMLTarget)}

        # task's metadata
        metadata = {p.name: task.pop(p) for p in list(task) if isinstance(p, YAMLMeta)}

        # add tagless programs
        tagless = [item for item in task if isinstance(item, str)]
        aliases = {}
        for name in tagless:
            if not name in preset_programs:
                continue
            item = task.pop(name)
            aliases = {p: name for p in preset_programs[name]}
            programs.update({p: item for p in preset_programs[name]})

        # remaining arguments are common parameters
        common_params = task

        # fill target's attachment
        for id in task_ids:
            for target, attachment in targets.items():
                attachments.setdefault(Target(target, *id), {}).update(attachment)
            for item, value in metadata.items():
                attachments.setdefault(id, {}).update({item: value})

        # fill tasks with parameter values
        for program_name in list(programs):
            parameters = programs[program_name]
            if not parameters:
                parameters = {}

            # add program parameters (global, common, and local)
            parameters = {
                **global_params.get(aliases.get(program_name), {}),
                **global_params.get(program_name, {}),
                **common_params,
                **parameters,
            }

            # fix parameter names
            # parameters = normalize(parameters)
            try:
                parameters = process_parameters(parameters)
            except Exception as exc:
                raise BatchFileError(f"Something went wrong in {taskname}:\n{exc}")

            # handle meta instruction
            if YAMLMacro("COPY") in parameters:
                copy_from = parameters.pop(YAMLMacro("COPY"))
                if copy_from in preset_programs:
                    parameters = {
                        **programs[preset_programs[copy_from][0]],
                        **parameters,
                    }
                elif copy_from in programs:
                    parameters = {**programs[copy_from], **parameters}
                else:
                    # skip
                    continue

            if YAMLMacro("CONDITION") in parameters:
                # eval expression
                expr = parameters.pop(YAMLMacro("CONDITION"))
                ans = eval(expr, None, parameters)
                if not ans:
                    # skip
                    continue

            invalid_keys = {key for key in parameters if not isinstance(key, str)}
            if invalid_keys:
                raise BatchFileError(f"Found invalid parameter keys: {invalid_keys}")

            # update programs for future use
            programs[program_name] = parameters

            if not task_ids:
                continue

            # special case: path
            if "path" in parameters:
                try:
                    path = pathlib.Path(
                        parameters["path"]
                        .replace("\\", os.path.sep)
                        .replace("/", os.path.sep)
                    )
                    if not path.is_absolute():
                        path = (path_prefix / path).resolve()
                except:
                    raise BatchFileError(
                        f"In task '{taskname}', invalid path: '{parameters['path']}'"
                    )

                if check_path and not path.exists():
                    raise BatchFileError(
                        f"In task '{taskname}', path does not exist: {path}"
                    )
                parameters["path"] = str(path)

            # make task
            task = {
                "program": program_name,
                "task": taskname,
                "identifiers": task_ids,
                "input_indices": input_indices,
                "input_branches": input_branches,
                "output_indices": output_indices,
                "output_branches": output_branches,
                "parameters": parameters,
            }
            tasks.append(task)
            indexparser.update_identifiers(task_ids)
    # update tasks
    if modifiers:
        tasks = apply_modifiers(tasks, modifiers)
    return tasks, attachments


class YAMLKey(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"{self.yaml_tag} {self.name}"

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            raise TypeError(f"Cannot compare {type(self)} with {type(other)}")
        return self.name == other.name

    @classmethod
    def to_yaml(cls, dumper, data):
        return dumper.represent_scalar(cls.yaml_tag, data.name)

    @classmethod
    def from_yaml(cls, loader, node):
        value = loader.construct_scalar(node)
        return cls(value)


class YAMLTarget(YAMLKey):
    yaml_tag = "!target"


class YAMLProgram(YAMLKey):
    yaml_tag = "!program"


class YAMLIdentifiers(YAMLKey):
    yaml_tag = "!ids"


class YAMLTask(YAMLKey):
    yaml_tag = "!task"


class YAMLMacro(YAMLKey):
    yaml_tag = "!macro"


class YAMLMeta(YAMLKey):
    yaml_tag = "!meta"


# def normalize(obj):
#     """normalize parameter names"""

#     def tame(name):
#         if isinstance(name, YAMLKey):  # ignore yaml tags
#             return name
#         return name.strip().replace("-", "_")

#     if isinstance(obj, (tuple, list)):
#         return type(obj)(tame(name) for name in obj)
#     elif isinstance(obj, dict):
#         return {tame(key): value for key, value in obj.items()}
#     else:
#         return tame(obj)


#
# template

RE_TEMPLATE = re.compile(r"(<[^><]+>)")


def generate_template(template, cases):
    """generate items from template by substituting value combinations"""
    if not isinstance(cases, list):
        cases = [cases]
    combinations = combine_variables(*cases)
    new = {}
    for item in template:
        if not is_template(item):
            new[item] = template[item]
            continue
        for combination in combinations:
            filled = fill_template({item: template[item]}, combination)
            duplicates = set(filled) & set(new)
            if duplicates:
                msg = f"Found duplicated items in solved template: {duplicates}"
                warnings.warn(msg)
            new.update(filled)

    return new


def is_template(template):
    """check if object is template"""
    if isinstance(template, str) and RE_TEMPLATE.search(template):
        return True
    elif isinstance(template, YAMLKey) and RE_TEMPLATE.search(template.name):
        return True
    elif isinstance(template, list):
        if any(is_template(item) for item in value):
            return True
    elif isinstance(template, dict):
        if any(is_template(key) for key in template):
            return True
        elif any(is_template(value) for value in template.values()):
            return True
    return False


def fill_template(template, values):
    """fill template with values"""
    if isinstance(template, str):
        # string key
        new = template
        for variable in RE_TEMPLATE.findall(template):
            name = variable[1:-1]
            if not name in values:
                raise ValueError(f"Unknown template variable: {name}")
            if template == variable:
                # replace value directly (no casting)
                new = values[name]
            else:
                # replace part of string (cast to string)
                new = new.replace(variable, str(values[name]))
        return new
    elif isinstance(template, YAMLKey):
        # YAML key
        cls = type(template)
        return cls(fill_template(template.name, values))
    elif isinstance(template, list):
        # list of values
        return [fill_template(item, values) for item in template]
    elif isinstance(template, dict):
        # dictionary
        return {
            fill_template(key, values): fill_template(template[key], values)
            for key in template
        }
    else:
        return template


def combine_variables(*dicts):
    """generate value combinations"""
    combinations = []
    for dct in dicts:
        keys = list(dct)
        cases = [
            items if isinstance(items, list) else [items] for items in dct.values()
        ]
        numcase = max(map(len, cases))
        cases = [case * numcase if len(case) == 1 else case for case in cases]
        if len(set(map(len, cases))) != 1:
            raise ValueError(f"Incombatible number of values in {dct}")
        # unique value combinations
        combs = [dict(zip(keys, instances)) for instances in zip(*cases)]
        # update combinations
        if not combinations:
            combinations = combs
        else:
            combs1 = [{**d2, **d1} for d1, d2 in itertools.product(combinations, combs)]
            combs2 = [{**d1, **d2} for d1, d2 in itertools.product(combinations, combs)]
            combinations = []
            for dct in combs1 + combs2:
                if not dct in combinations:
                    combinations.append(dct)
    return combinations


#
# post process parameters


def process_parameters(parameters):
    """process parameters"""
    processed = {}
    for key, value in parameters.items():
        # tame name
        try:
            name = process_parameter_name(key)
        except:
            raise ValueError(f"Invalid parameter name: {key}")
        try:
            value = process_parameter_value(value)
        except:
            raise ValueError(f"Invalid parameter value: {name}: {value}")
        processed[name] = value
    return processed


def process_parameter_name(name):
    if isinstance(name, YAMLKey):
        # ignore yaml tags
        return name
    return name.strip().replace("-", "_")


def process_parameter_value(value):
    """process list (auto complete)"""
    if not isinstance(value, list):
        return value
    # auto complete values
    return auto_complete(value)


def auto_complete(lst, placeholder="..."):
    """auto complete list of string, using a place holder"""
    while True:
        try:
            index = lst.index(placeholder)
        except ValueError:
            return lst

        if index in (0, len(lst) - 1):
            raise ValueError(
                f"Missing lower or upper value for auto-complete in: {lst}"
            )
        prev = lst[index - 1]
        next = lst[index + 1]
        match1 = re.match(r"^(.*?)(\d+)$", str(prev))
        match2 = re.match(r"^(.*?)(\d+)$", str(next))
        if match1 is None or match2 is None:
            raise ValueError(
                f"Cannot auto complete values with no number: {prev}, {next}"
            )
        prefix1 = match1.group(1)
        prefix2 = match2.group(1)
        if prefix1 != prefix2:
            raise ValueError(
                f"Cannot auto complete values with unmatched prefixes: {prev}, {next}"
            )
        first = int(match1.group(2))
        last = int(match2.group(2))
        values = range(first + 1, last) if last >= first else range(first - 1, last, -1)
        lst = lst[:index] + [f"{prefix1}{num}" for num in values] + lst[index + 1 :]


#
# apply task modifiers


def apply_modifiers(tasks, modifiers):
    """update task list from modifiers"""
    new = []
    for task in tasks:
        for modifier in modifiers.values():
            _task = task.copy()
            branch = modifier.get("branch", None)
            if branch:
                if not task["output_branches"]:
                    _task["output_branches"] = branch
                else:
                    _task["output_branches"] = task["output_branches"] + branch
            if (name := task["program"]) in modifier:
                params = modifier[name].copy()
                if "program" in params:
                    _task["program"] = params.pop("program")
                _task["parameters"] = {**task["parameters"], **params}
                new.append(_task)
    return new
