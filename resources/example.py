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


# create a toolbox
toolbox = ma.Toolbox("cool-functions", description="A collection of cool functions.")
toolbox.add_program("cool-1", a_cool_function)
toolbox.add_program("cool-2", another_cool_function)

if __name__ == "__main__":
    # parse arguments
    toolbox.cli()
