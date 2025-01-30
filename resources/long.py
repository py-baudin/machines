import time
import machines as ma


@ma.machine()
@ma.parameter('duration', float, default=5, help='duration in seconds')
def long_program(duration):
    print(f'Doing something for {duration}s')
    time.sleep(duration)

toolbox = ma.Toolbox('Long')
toolbox.add_program('run', long_program)

if __name__ == '__main__':
    toolbox.cli()
