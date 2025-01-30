""" Demonstrate the various parameters and inputs """
import machines as ma

@ma.machine()
@ma.output('output', ..., default=None, help='An optional variable output target\'s name')
@ma.parameter('flag', ma.Flag(enable='mult'), default=False, help='An optional yes/no flag')
@ma.parameter('choice', ma.Choice(['bar', 'baz']), help='A required choice')
@ma.parameter('switch', ma.Switch({'foo': 'Foo', 'goo': 'Goo'}), default='foo', help='An optional switch')
def generate(flag, choice, switch):
    """ generate some data """
    data = switch + choice
    if flag:
        data = data * 2
    print(f'data={data}')
    return {'data': data}

@ma.machine()
@ma.input('input1', ['A', 'B'], default='A', help='A variable input target')
@ma.input('input2', {'a': 'A', 'b': 'B'}, default='b', help='A variable input target')
def test(input1, input2):
    print(f'input1: {input1["data"]}')
    print(f'input2: {input2["data"]}')




toolbox = ma.Toolbox('Example 3 toolbox')
toolbox.add_program('generate', generate)
toolbox.add_program('test', test)

if __name__ == '__main__':
    toolbox.cli()
    

