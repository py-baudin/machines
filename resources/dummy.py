import time
import machines as ma

# init toolbox
toolbox = ma.Toolbox("dummy", description="A dummy toolbox for testing purposes.")

"""
Create a very simple multi-machine program.

Notes:
- this program creates temporary intermediary data (target1 and target2)
    which are deleted once the program has completed
- handling of data files is not necessary (a default handler based on pickle is used)
    because the program has no output (only prints out information)

"""


@ma.machine(output="target1")
@ma.parameter("value", default="foobar")
def simple1(value):
    """ creates target1 data with value "foobar" """
    print(f"Initial value: {value}")
    return value


@ma.machine(inputs="target1", output="target2")
def simple2(target1):
    """ creates target2 by modifying target1 """
    return target1.upper()[::-1]


@ma.machine(inputs="target2")
def simple3(target2):
    """ print target2 """
    print(f"Final value: {target2}")


# create multi-machine program
toolbox.add_program(
    "simple", [simple1, simple2, simple3], help="A very simple program."
)


"""
Create a dummy eco-system of programs

Run the following commands:

    #
    # create initial data with identifiers "id1~", "id1~br1" and "id2~" and
    # with diffent values (set via parameter "value")
    python dummy.py dummy-reg --value bar id1
    python dummy.py dummy-reg --value baz id1~br1
    python dummy.py dummy-reg --value baz id2


    #
    # run mapping program only on id1#A and id2#A (not id1#A~br1)
    python dummy.py dummy-run *~


    #
    # run multi-input mapping program on identifier "id1~br1" and setting variable input X to B
    python dummy.py dummy-multi --X B id1~br1

    # output:
    A (branch=br1): baz
    B (branch=None): FOOBARBAR # <--- branch fallback (id1#B~br1 does not exist)


    #
    # run aggregate program on existing targets (id1#A, id1#A~br1, id2#A, id1#B, id2#B)
    python dummy.py dummy-agg .

    # the output is a dict containing of the data:
        {
            "Identifier(index='id1', branch='br1')": {'A': 'baz', 'B': None},
            "Identifier(index='id2', branch=None)": {'A': 'baz', 'B': 'FOOBAZBAZ'},
            "Identifier(index='id1', branch=None)": {'A': 'bar', 'B': 'FOOBARBAR'}
        }


Demonstrates:

- Using a default handler
    All targets use this handler unless specified otherwise

- Variable target type (VariableIO)
    The input data is set by a user parameter

- Branch fallback
    If a branch is missing on one of the inputs, a parent branch is used instead

- Aggregate on identifiers / requires "any"
    All matching inputs are passed to the function and a single targets is returned.
    Other possible aggration modes: "index" (combine inputs from all indices / same branch)
    and "branch" (combine inputs from all branches / same index)


"""

# set default handler
toolbox.default_handler = ma.json_handler

@ma.machine(output="A")
@ma.parameter('value', ma.Choice(['bar', 'baz']), default='bar')
def dummy_init(value):
    """ Dummy init program (initialize identifiers). """
    print(f"value: {value}")
    return value


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


@ma.machine(output="C")
@ma.input('A')
@ma.input('X', variable=True)
def dummy_multi(A, X):
    """ Dummy program with multiple inputs """
    targets = ma.get_context().targets
    print(f"A (branch={targets['A'].branch}): {A}")
    print(f"X (branch={targets['X'].branch}): {X}")
    return (A, X)


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


# add  programs
toolbox.add_program("dummy-init", dummy_init)
toolbox.add_program("dummy-run", [dummy_run1, dummy_run2], help="Dummy run program.")
toolbox.add_program("dummy-multi", dummy_multi)
toolbox.add_program("dummy-agg", dummy_agg)

toolbox.add_signature(".dummy", hash="$HASH", date="$DATE")


if __name__ == "__main__":
    toolbox.cli()
