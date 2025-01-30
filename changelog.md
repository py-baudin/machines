# `machines` changelog

## version 1.4.dev8
- parsers: fixed path prefix
- fixed cli (YAMLStrings)
- fixed decorators (when using ma.TargetType)
- fixed examples (use new decorator syntax)

## version 1.4.dev7
- cli: added option "--alias"
  To avoid having to modify the batch file with new aliases

## version 1.4.dev6
- cli: added "--default" toolbox option
- parsers: better error message in parameter processing
- parsers: in batch files, added auto complete in lists with `...` placeholder
- parsers: cleanup useless macro

## version 1.4.dev5
- refactor parameters and variable IOs. Add example3

## version 1.4.dev4
- parameters: Config type produces a ConfigFile dict

## version 1.4.dev3
2023-04-18
- task/machines: disable branch fallback if `requires=='any'`
- fixed bug in test_parsers

## version 1.4.dev2
2023-03-29

- cli/parser:
  - new batch CONFIG options: `INCLUDE` and `TASKS`
  - `INCLUDE` merge other batch files to the current one
  - `TASKS` modify existing tasks

```yaml
CONFIG:
  TASKS:
    task1: {branch: branch1, alias1: {program: prog1, param1: value1, ...}}
    task2: ...
  INCLUDE:
    - file1.yml
    - file2.yml
```



## version 1.3.22
- cli/parser
  - new batch CONFIG option: `GENERATE`
  - option `--dry` only shows tasks with non temporary targets
- factory:
  - handle keyboard interrupt (ctrl-c)

  ```yaml
CONFIG:
  GENERATE:
    # this generates 2 versions of the tasks (value1, branch1) / (value2, branch2)
    param: [value1, value2]
    branch: [branch1, branch2]

task1~<branch>:
  param: <param>

  ````

## version 1.3.15 -> 1.3.21 (catching up)

- cli:
  - allow `...` in multi-valued parameters ```series: [1,2,...,10]```
  - `summary` can store output to csv
  - `summary --invalid` shows invalid target pathes
- better decorators:

```python
ma.machine()
ma.input(input1, target)
ma.output(target)
ma.parameter(param1, type, default=..., )
def my_machine(input1, param1):
  ...
```


## version 1.3.15
2022-02-10

cli.py: important refactor

- fixed indentifiers/targets syntax issues
- moved IndexParser and parse_batch to parsers.py
- added unittests (various fixes)
- added macros in batch files
  '''yaml
    # copy parameters from another program
    !macro COPY: <program-name>
    # run on condition a parameter is defined
    !macro EXIST: <parameter-name>
    # run if condition on parmaeters is met
    !macro CONDITION: <expression>
  '''
