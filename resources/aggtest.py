import itertools
import logging
from machines import machine, Toolbox, factory, get_context


@machine(output="Foo")
def machineFoo():
    pass


@machine(output="Bar")
def machineBar():
    pass


@machine(inputs=["Foo", "Bar"], output="Foobar", aggregate="ids", requires="all")
def machineFoobar(Foo, Bar):
    context = get_context()
    targets_Foo = context.targets["Foo"]
    targets_Bar = context.targets["Bar"]
    results = {target.index: {"Foo": [], "Bar": []} for target in targets_Foo + targets_Bar}
    for target, value in zip(targets_Foo, Foo):
        results[target.index]["Foo"].append(target.index)

    for target, value in zip(targets_Bar, Bar):
        results[target.index]["Bar"].append(target.index)

    return results


# logging.basicConfig(level=logging.INFO)

with factory(hold=True) as fy:
    machineFoo(indices=[1, 2, 2], branches=[None, None, "br1"])
    machineBar(indices=[1, 2, 3], branches=[None, "br1", "br1"])

    identifiers = list(itertools.product([1, 2, 3], [None, "br1"]))
    task = machineFoobar.single(identifiers=identifiers)


table = task.output_data
for id in table:
    print(f"id: {id}")
    print(f"\t{table[id]}")
