# machines

> A minimal python library for creating and running batch tasks

## Rationale
In my work, I often need to implement new processing algorithms.
This typically means trying out various parameters combinations until the correct ones are found,
and adding new test data along the way to see how the function behaves on new cases.

As the project goes, my project folder becomes more and more clutered with many
poorly documented scripts and a large number of badly named subdirectories containing the results.
I end up spending more time than I wish on writting batch scripts and sorting my data.

This library is dedicated to helping badly-organized people like me to:
- quickly implement command-line programs
- simply handle batch tasks
- automatically organize and manage the data subfolders


## Simple example

Assuming you already implemented a new function that does some stuff,
here is a simple example on how to transform it into a batch-ready command line program.
Let's create a script:

```python
# example.py
import machines as ma

# Define a name and file format (json) for the output data
SomeData = ma.TargetType("some_data", handler=ma.json_handler)

# create program by decorating a function
@ma.machine()
@ma.output(SomeData)
@ma.parameter('param1', str)
def a_cool_function(param1):
    """ This is a function that does cool stuff """
    return "cool stuff with " + param1

# create a toolbox
toolbox = ma.Toolbox("cool-functions", description="A collection of cool functions.")
toolbox.add_program("cool-1", a_cool_function)

if __name__ == "__main__":
    # parse arguments
    toolbox.cli()


```

This very short program creates a command-line program with several options
already implemented. To discover these options, you can type the following command:

```
# display help string of the "cool-functions" toolbox
> python example.py --help

# display help string of the "cool-1" program
> python example.py cool-1 --help
```

To run the program `cool-1` on index "id1", with parameter "param" given the value "foo"
```
> python example.py cool-1 --param=foo id1

Task: cool-1 -> id1#some_data is done.
```

The returned output means that some output data of type `some_data`
were generated for the identifer `id1` (more on index and identifiers later).

Let's run it again, but this time with a `--branch` option added:

```
# add branch "br1" to output target
> python example.py cool-1 --branch=br1 --param=phoo id1

Task: cool-1 -> id1#some_data~br1 is done.
```

A quick look in the current directory shows a new directory tree with root `work`:

```
work\                 # new work directory
  id1\                # subfolder for index "id1"
    some-data\        # subfolder for data type "my-data"
      data.json       # json file containing: "cool stuff with foo"
    some-data~br1\    # same as above, but on branch "br1"
      data.json       # json file containing: "cool stuff with phoo"

```

We can use the generated data to create more cool stuff. Let's update our script:

```python
# `example.py`

...

# another data (also stored in json)
SomeOtherData = ma.TargetType("some_other_data", handler=ma.json_handler)

# create another program
@ma.machine()
@ma.input(SomeData)
@ma.output(SomeOtherData)
@ma.parameter('param2', str, default="bar")
def another_cool_function(some_data, param2):
    """ This is another function that does more cool stuff """
    return some_data + param2

...

# add second program to the toolbox
toolbox.add_program("cool-2", another_cool_function)

```

And run it with the rather minimalistic command:
```
> python example.py cool-2 .
```

Notice how I did not pass a value for parameter `param2`: a default value `bar` will be used,
and also that I used  `.` instead of an identifier like before: it means that all existing identifiers
will be used.

A look at the diretory tree shows two new data subfolders:

```
work\                   
  id1\                
    some_data\         
      data.json        
    some_data~br\       
      data.json         
    some_other_data\      # subfolder for data type "some_data"
      data.json           # json file containing: "COOL STUFF WITH FOOBAR"
    some_other_data~br1\  # anoter subfolder for data type "some_data"
      data.json           # json file containing: "COOL STUFF WITH PHOOBAR"
```

Et voilà.


For fun, let's run `example.py` without any argument. We get a standard command-line
usage documentation, including the newly written functions `cool-1` and `cool-2`:

```
Usage: example.py [OPTIONS] COMMAND [ARGS]...                                                      

  A collection of cool functions.                                                                  

Options:                                                                                           
  -d, --workdir PATH           Set main work directory (default: 'work')                           
  -t, --targetdir TARGETDIR    Setup a dedicated target directory. Syntax:                         
                               '<target-name>=<path-to-dir>' (repeat option                        
                               for additional target dirs).                                        

  --versioning [int|date]      Add version to target directories.                                  
  -s, --separators SEPARATORS  Set target path separators. Syntax:                                 
                               <PRIMARY><SECONDARY><ID><BRANCH>.                                   

  -v, --verbose                Enables verbose mode.                                               
  --config CONFIG              Set parameters via a YAML configuration file                        
                               (default file name: cool-functions.yml).                            

  --help                       Show this message and exit.                                         

Commands:                                                                                          
  cool-1  This is a function that does cool stuff   
  cool-2  This is another function that does more cool stuff                                               
  _       Various utilities.                                                                       
```

If you are wondering what's under `_  Various utilities`, let's type `python example.py _`.
We get a series of preset functions that help
you manage the data in your `work` directory and some other utilities.


```
Commands:
  info       Display info on current toolbox.
  cleanup    Cleanup temporary directory
  summary    List existing targets.
  location   Return the path of existing targets.
  remove     Remove existing targets.
  view       View targets (open in file system).
  batch      Run commands in batch file FILE.
```


## Targets

In `machines`, data generated by the programs are stored in dedicated subfolders.
The identification and management (viewing, removal, searching)
of the datasets is based solely on the folder names.
In that sense, `machines` uses a very primitive database management system, but
one that does not rely on a database file (such as SQlite).

Three components are needed to determine the storage path of a piece of data
(existing or to be generated), aka a "target":

- a target's name (generally the name of the innermost subfolder)
- a target's index (generally the sequence of parent subfolders)
- a target's branch (optional, similar to a version number,
    generally contained in a suffix to the innermost subfolder)

For example, the path: `site1/subject1/visit1/data~v1/` could correspond to a
target whose name, index and branch are `data`, (`site1`, `subject1`, `visit1`)
and `v1`, respectively.

The correspondance between a target's name and identifiers and a system path is
not fixed and can be adapted to one's needs.

Although very poor in terms of search speed and flexibility, this choice reduces
its footprint in the filesystem (no dedicated files to maintaining the database),
makes the outputs easy to browse manually, and renders `machines` adaptable on
preexisting subfolder trees.


## Target identifiers

A Target's identifier is the combination of an `index`,
which is an identification string for a piece of data (such as a patient's id),
and a `branch`, which serves as a custom version number for of the dataset.

Identifiers are used in command lines arguments (and in batch files) to designate
which target(s) to generate or use as inputs. Some tricks are implemented
to flexibly and rapidly select multiple identifiers.

Using the above module `example.py`, here are a few examples:
```
# run cool-2 on all inputs having identifier 'id1~' (index=id1, branch=<not set>)
> python example.py cool-2 id1

# run cool-2 on all inputs having identifier 'id1~br1' (index=id1, branch=br1)
> python example.py cool-2 id1~br1


# wildcards

# run cool-2 on all inputs matching string 'id1*'
> python example.py cool-2 id1*

# run cool-2 on all inputs having *branch* 'br1'
> python example.py cool-2 *~br1


# identifier swap

# run cool-2 on all inputs and *append* branch br2 to output targets' identifiers
> python example.py cool-2 --branch=br2 .

# run cool-2 on identifier 'id1~' and set output index to 'id3'
> python example.py cool-2 --index=id3 id1

```

## Advanced use

In the following, we will build a complete ecosystem of dummy programs to illustrate
a number of `machines` features.

Let's write a first program to generate some data (i.e no input, only parameters):

```python
# dummy.py

import time
import machines as ma

# init toolbox
toolbox = ma.Toolbox("dummy", description="A dummy toolbox for testing purposes.")

# set default handler
toolbox.default_handler = ma.json_handler

@ma.machine(output="A")
@ma.parameter('value', ma.Choice(['bar', 'baz']), default='bar')
def dummy_init(value):
    """ Dummy init program (initialize identifiers). """
    print(f"value: {value}")
    return value


# add program
toolbox.add_program("dummy-init", dummy_init)

...

if __name__ == "__main__":
    toolbox.cli()
```

Note:

- A default file-handler is set for all target types.
  This allows using simple strings as target types directly in the `@machine` decorator
- A parameter class `Choice` is used, that accepts only a set of values.
- Most parameter types accept a `default` option.


Let's run this program:

```
# create initial data with identifiers "id1~", "id1~br1" and "id2~" and with diffent values (set via parameter "value")
> python dummy.py dummy-init --value=bar id1
> python dummy.py dummy-init --value=baz id1~br1
> python dummy.py dummy-init --value=baz id2
```

A summary of what was sucessully run can be viewed by typing:

```
> python dummy.py _ summary
Storage(work) [3]
        id1#A
        id1#A~br1
        id2#A
```


Next, let's define a dummy program that takes `dummy-init`'s output as input:

```python

@ma.machine(inputs="A", output="B1")
@ma.parameter("duration", float, default=0.1)
def dummy_run1(A, duration):
    """ Dummy run (1/2). """
    # sleep for a while
    time.sleep(duration)

    # modify input data
    value = A.upper()

    print(f"value: {value} (duration: {duration:.1f}s)")
    return value


@ma.machine(inputs="B1", output="B")
@ma.parameter("multiply", int, default=2)
def dummy_run2(B1, multiply):
    """ Dummy run (2/2). """
    # modifyin put data
    value = "FOO" + B1 * multiply

    print(f"value: {value} (multiply: {multiply})")
    return value

# add program
toolbox.add_program("dummy-run", [dummy_run1, dummy_run2], help="Dummy run program.")

```

Note:

- Program `dummy-run` is made of two functions.
  The combination of both programs in the `add_program` function creates a
  "meta-Machine", ie. a Machine that returns other Machines. More complicated
  programs can be created by explicitely defining meta-Machine functions
  (using the decorator `@metamachine`).


Let's run this program on some of the existing identifiers:

```
# run mapping program only on id1#A and id2#A (not id1#A~br1)
> python dummy.py dummy-run *~
```

Let's continue with another programs that requires two inputs to run:

```python

@ma.machine(output="C")
@ma.input('A')
@ma.input('X', variable=True)
def dummy_multi(A, X):
    """ Dummy program with multiple inputs """
    targets = ma.get_context().targets
    print(f"A (branch={targets['A'].branch}): {A}")
    print(f"X (branch={targets['X'].branch}): {X}")
    return (A, X)


# add program
toolbox.add_program("dummy-multi", dummy_multi)
```

Note:

- Multi_dummy has two input types (`B` and `X`)
- Input `X` is also declared as "variable".
  It means that input type `X` is determined by the user.
- The function uses a context object (`ma.get_context()`) to access input identifiers


Let's run this program:

```
# run multi-input mapping program on identifier "id1~br1" and setting variable input X to B
> python dummy.py dummy-multi --X B id1~br1

A (branch=br1): baz
X (branch=None): FOOBARBAR  # <--- branch fallback
```

Note:

- Although we asked for inputs having identifier `id1~br1`, the program used
  the value of target `id1#B~` for input `X` instead, as the target `id1#B~br1`
  was not available. The mechanism where a parent branch is used instead of the
  demanded branch is called "branch fallback".
- Branch fallback can be deactivated at the command-line:
  ```
  > python dummy.py dummy-multi --X B --no-fallback id1~br1

  Task: dummy-multi -> id1#C~br1 is pending (missing inputs: X)
  # the task cannot be run due to missing input id1#B~br1
  ```


Finally, let's define an "aggregate" program that will combine the data of
inputs with different identifiers:

```python

# dummy aggregate
@ma.machine(inputs=["A", "B"], output="D", aggregate=True, requires="any")
def dummy_agg(A, B):
    """ Dummy aggregate (merge identifiers). """
    context = ma.get_context()
    identifiers = context.identifiers
    attachments = context.attachments
    aggregated = {}

    for id, value, attach in zip(identifiers['A'], A, attachments['A']):
        key = "{}~{:3}".format(id.index, id.branch if id.branch else "")
        value = value + attach.get("foobar", "")  # update value with attachment
        aggregated.setdefault(key, {"A": None, "B": None})["A"] = value

    for id, value, attach in zip(identifiers['B'], B, attachments['B']):
        key = "{}~{:3}".format(id.index, id.branch if id.branch else "")
        value = value + attach.get("foobar", "")  # update value with attachment
        aggregated.setdefault(key, {"A": None, "B": None})["B"] = value

    for id, value in aggregated.items():
        print(f"{id}: {value}")
    return aggregated

# add program
toolbox.add_program("dummy-agg", agg_dummy)
```

Note:

- Option: `aggregate=True` passes inputs from all
  identifiers to the function. A single target is created.
  Other possible aggration modes: `index` (combine inputs from all indices / same branch)
    and `branch` (combine inputs from all branches / same index)
- When the `aggragate` option is set, input variables `A` and `B`
  become lists of values (one for each identifier)
- Option `requires="any"` passes all matching inputs to the function.
  When `requires="all"` (default), only inputs for which both targets (`A` and `B`)
  exist are passed to the function.
- Arguments `attachment_<input>` are special variable that can be used to attach
  some data to the input targets. This is especially useful when used in combination with batch files (see below).

The program is run with the command:

```
> python dummy.py dummy-agg --attach='id2#B~: {foobar: "!"}' .

id1~   : {'A': 'bar', 'B': 'FOOBARBAR'}
id1~br1: {'A': 'baz', 'B': None}          # <---- note missing value for target id1#B~br1
id2~   : {'A': 'baz', 'B': 'FOOBAZBAZ!'}  # <---- note attachment value "!"
```


## batch files
All commands can be put in a batch file. The following YAML file will
produce the same output as the above commands:

```yaml
# resources/batch_syntax.yml

!task init-1:
  inputs: id1
  dummy-init:
    value: bar

!task init-1-br1:
  inputs: id1~br1
  dummy-init:
    value: baz

!task init-2
  inputs: id2
  dummy-init:
    value: baz
  !target B:
    # add the following attachment to target id2#B~
    foobar: "!"

!task run:
  inputs: "*~" 
  # note: a parameter "outputs" can also be set if needed
  dummy-run:
    # this program does not require any parameter

!task multi:
  inputs: id1~br1
  dummy-multi:
    X: B # variable target input

!task aggregate:
  inputs: .
  dummy-agg:
    # this program does not require any parameter

```

The command to run the batch file is:

```
> python dummy.py _ batch resources/dummy.yml
```

Note:

- It is needed to run the batch command twice, as some input indices include
  wildcards ("*", ".") which can only be resolved if some targets already exist.
- it is possible to restrict the batch run to a subset of the identifiers by
  appending the identifiers to the command line after the batch filename

## Work directory, target directories and config file

By default, all data are stored in a subdirectory of the work directory (`work` by default).

It is often preferable to store some of the targets in a separate directory.
Such a directory is reserved for the type of data in question and is called a
"target directory".

Both the workdir and the target directories can be set at the command line:

```
# set the workdir to "dummy/work" and a targetdir for targets A to "dummy/A"
> python dummy.py -d dummy/work -t A=dummy/dirA dummy-init <id>

# The summary command shows the storage locations of the targets
> python dummy.py -d dummy/work -t A=dummy/dirA _ summary

Storage(dummy/work) [4]
        id1#B
        id1#C~br1
        id2#B
        _#D
Storage(dummy\dirA) [3]
        id1#A
        id1#A~br1
        id2#A

```

These settings can be fixed in a configuration file:

```yaml
# resources/dummy.yml

workdir: dummy/work
targetdirs: [A=dummy/dirA]

```

And used with the option `--config`:

```
> python dummy.py --config resources/batch.yml _ summary
```

Note:

- If the configuration file has the same name as the toolbox and is present
in the current directory, it will be automatically loaded.



## Other useful options

Programs added to the toolbox have a number of options automatically implemented.

The `--dry` option shows the tasks to be run without actually running them:
```
> python example cool-2 --dry <id>
```

The `-o/--overwrite` option allows overwriting existing output targets
(otherwise, tasks with existing output are skipped).

```
> python example.py cool-2 --overwrite/-o <id>
```

Disable branch fallback (see example above):
```
> python example.py cool-2 --no-fallback <id>
```


## On target types

The default file handler uses `pickle` to store the objects: `pickle_handler`.
A `json` handler is also provided: `json_handler`.

Custom file-handlers can be created using the function `file_handler`.

```python
from machines import file_handler

# custom file handler
def save_function(dirname, data):
    """ save data to directory dirname """
    ...
def load_function(dirname):
    """ save data to directory dirname """
    ...
handler = file_handler(save=save_function, load=load_function)

# from a list of file handlers (all will be run sequentially on the dirname and data)
handler = file_handler([file_handler1, file_handler2])
```
