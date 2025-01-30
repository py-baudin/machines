""" How to operates with machines in the code """

from machines import machine, factory, MemoryStorage, Target


@machine(output="A", param=["bar", "baz"])
def machineA(param):
    return "foo" + param

@machine(inputs="A", output="B")
def machineB(A):
    return A.upper()

# define storage location for targets
# use 'FileStorage' if data is to be stored on disk
storages = {"A": MemoryStorage(), "B": MemoryStorage()}

# run tasks in factory
with factory(hold=True, storages=storages):
    tasksA = machineA("id1", param="baz")
    tasksB = machineB(["id1", "id2"])

# check tasks
assert tasksA[0].status.name == "SUCCESS"
assert tasksB[0].status.name == "SUCCESS"
assert tasksB[1].status.name == "PENDING"

# check storage data
assert storages["A"].exists(Target("A", "id1"))
assert storages["A"].read(Target("A", "id1")) == "foobaz"

assert storages["B"].exists(Target("B", "id1"))
assert storages["B"].read(Target("B", "id1")) == "FOOBAZ"
assert not storages["B"].exists(Target("B", "id2"))
