""" Command line tool """

import os
import shutil
import collections
import logging
import yaml
import traceback
import click
import csv
from pathlib import Path

from . import __version__, utils, parameters
from .common import ExpectedError, InvalidTarget, RejectException, TargetIsLocked
from .parameters import ParameterError
from .session import setup_storages, setup_storage, Session
from .target import Target, Identifier, Branch
from .parsers import IndexParser, parse_batch, IndexParserError, BatchFileError

# from .idparser import LocalIndexParser

LOGGER = logging.getLogger(__name__)

# default tempdir name
TEMP_DIR = ".machines"

# click context settings
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"], show_default=True)


def get_tempdir(workdir, subdir=None):
    """get tempdir for a given workdir"""
    if not subdir:
        subdir = ""
    return Path(workdir) / TEMP_DIR / subdir


def ctxset(key, value):
    """set value in context"""
    ctx = click.get_current_context()
    if not ctx.obj:
        ctx.obj = {}
    ctx.obj[key] = value


def ctxget(key, default=None):
    """get context object"""
    objects = click.get_current_context().obj
    if not objects or not key in objects:
        return default
    return objects[key]


def setup(toolbox, module=None):
    """setup command-line tool"""

    # add default configuration
    default_config = toolbox.meta.get("DEFAULT_CONFIG")
    if not default_config:
        default_config = Path(toolbox.name.replace(" ", "_").lower()).with_suffix(
            ".yml"
        )
        toolbox.meta["DEFAULT_CONFIG"] = default_config

    # fill context object with toolbox
    ctxobj = {"toolbox": toolbox, "module": module}

    @click.group(
        name=toolbox.name,
        help=toolbox.description,
        cls=SectionGroup,
        # invoke_without_command=True,
        no_args_is_help=False,
        context_settings={**CONTEXT_SETTINGS, "obj": ctxobj},
    )
    @click.option(
        "-d",
        "--workdir",
        metavar="path=WORKDIR,...",
        default="path=work",
        type=KeyValue(["!path", "struct", "index", "branch", "noindex", "nobranch"]),
        cls=OptionWithConfig,
        help=(
            "Set main work directory. "
            'Syntax: "key1=value1,key2=value2,etc.". '
            "Keys: path, struct, index, branch, noindex, nobranch."
        ),
    )
    @click.option(
        "-t",
        "--targetdir",
        "targetdirs",
        multiple=True,
        type=KeyValue(
            [
                "!name",
                "!path",
                "struct",
                "index",
                "branch",
                "noindex",
                "nobranch",
                "default_branch",
            ]
        ),
        cls=OptionWithConfig,
        metavar="name=TARGET,path=TARGETDIR,...",
        help=(
            "Setup a dedicated target directory "
            "(repeat option for additional target dirs). "
            'Syntax: "key1=value1,key2=value2,etc.". '
            "Keys: name, path, struct, index, branch, noindex, nobranch."
        ),
    )
    @click.option(
        "--lock",
        multiple=True,
        help="Lock targets to avoid overwritting by accident.",
        cls=OptionWithConfig,
    )
    @click.option(
        "-k",
        "--keep-intermediary",
        is_flag=True,
        cls=OptionWithConfig,
        help="Do not auto-remove intermediary targets.",
    )
    @click.option("-v", "--verbose", is_flag=True, help="Enables verbose mode.")
    @click.option(
        "-s",
        "--show-all",
        is_flag=True,
        help="Show all tasks, including temporary ones.",
    )
    @click.option("-e", "--stop-on-error", is_flag=True, help="Stop factory on error.")
    @click.option(
        "--default",
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=callback_default_toolbox,
        help="Force use of default toolbox.",
    )
    @click.option(
        "--alias",
        multiple=True,
        help="Program aliases for use in batch files (syntax: alias=program).",
    )
    @click.option(
        "--config",
        metavar="CONFIG",
        default=default_config,
        callback=callback_config,
        expose_value=False,
        is_eager=True,
        help=f"Set parameters via a YAML configuration file.",
    )
    @toolbox_options(toolbox)
    def cli(
        workdir,
        targetdirs,
        verbose,
        stop_on_error,
        keep_intermediary,
        lock,
        show_all,
        alias,
        **parameters,
    ):
        """Command-line interface for a toolbox"""
        # setup verbosity and storages
        if verbose:
            logging.basicConfig(level=logging.INFO)

        # initializers
        for init in toolbox.initializers:
            logging.info(f"Found initializer: {init}")
            init_params = set(parameters) & set(init.parameters)
            init_params = {name: parameters[name] for name in init_params}
            init.func(toolbox, **init_params)

        # setup storages
        workpath = Path(workdir["path"])
        tempdir = get_tempdir(workpath, workpath.stem)
        storages = setup_storages(
            toolbox,
            workdir,
            tempdir=tempdir,
            targetdirs=targetdirs,
            target_lock=lock,
        )
        LOGGER.info("Storages: %s", storages)

        # program aliases
        aliases = {name: [name] for name in toolbox.programs}
        for value in alias or []:
            aliasname, progname = value.split("=")
            if not progname in toolbox.programs:
                clean_exit(f"Unknown program: {program}.")
            aliases.setdefault(aliasname, []).append(progname)

        # make session
        auto_cleanup = not keep_intermediary
        session = Session(
            toolbox, storages, name=toolbox.name, auto_cleanup=auto_cleanup
        )

        # various options
        options = {"stop_on_error": stop_on_error, "show_all": show_all}

        ctxset("session", session)
        ctxset("options", options)
        ctxset("aliases", aliases)

        # end of cli group

    # add toolbox programs
    for group, programs in toolbox.groups.items():
        section = cli.add_section(group)
        for program in programs:
            command = program_cli(program, toolbox)
            cli.add_command(command, name=program, section=section)

    # add utilities
    @cli.group(name="_", cls=OrderedGroup)
    def utils():
        """Various utilities."""

    utils.add_command(utils_info())
    utils.add_command(utils_cleanup())
    utils.add_command(utils_summary())
    utils.add_command(utils_export())
    utils.add_command(utils_remove())
    utils.add_command(utils_view())
    utils.add_command(utils_batch())

    return cli


def program_cli(progname, toolbox):
    """make CLI for a program"""

    program = toolbox.programs[progname]
    info = program.info
    help = toolbox.programs_help[progname]
    manual = toolbox.programs_manual[progname]

    if not help:
        help = f"program {progname}."

    description = info["description"]
    if not description:
        description = help

    def ioprints(ios):
        if not ios:
            return "None"

        def ioprint(name):
            items = ios[name]
            if name in info["variable"]:
                return f"{name}: <user-defined>"
            # else
            stritems = []
            for item in items:
                if not item.get("type"):
                    stritems.append(f"{item['dest']}")
                else:
                    stritems.append(f"{item['dest']}:{item['type']}")
            return f"{name} ({' | '.join(stritems)})"

        return ", ".join(ioprint(name) for name in ios)

    usage = (
        description
        + f"""

        \b
        Select input targets with valid identifiers IDS.
        Valid index syntaxes: id, id~br, *id~*, [id1|id2], id~[br1|br2]

        \b
        input targets:
            {ioprints(info["inputs"])}
        \b
        output target:
            {ioprints(info["outputs"])}
    """
    )

    @click.command(
        name=progname,
        help=usage,
        short_help=help,
        context_settings=CONTEXT_SETTINGS,
        cls=CommandWithSections,
    )
    @click.argument("identifiers", metavar="IDS", nargs=-1)
    @click.option("--index", "output_indices", help="Output index")
    @click.option("--branch", "output_branches", help="Output branch.")
    @click.option(
        "-o", "--overwrite", is_flag=True, help="Set write mode to 'overwrite'."
    )
    @click.option(
        "--mode",
        type=click.Choice(["overwrite", "upgrade", "test"]),
        help="Write mode for existing data.",
    )
    @click.option("-F", "--no-fallback", is_flag=True, help="Disable branch fallback.")
    @click.option("-A", "--autorun", is_flag=True, help="Autorun parent programs.")
    @click.option("-D", "--dry", is_flag=True, help="Dry run, print dependency graph.")
    @click.option(
        "-a",
        "--attach",
        type=YAMLString(),
        multiple=True,
        help="Attach value to some targets (YAML string)",
    )
    @click.option(
        "--history", type=click.File(), help="File source/destination for history"
    )
    @program_options(program)  # add program options
    @program_manual(manual)  # add manual if any
    def program(
        identifiers,
        output_indices,
        output_branches,
        mode,
        overwrite,
        attach,
        autorun,
        dry,
        no_fallback,
        **parameters,
    ):
        """Run a program."""
        if not identifiers and not info["outputs"]:
            identifiers = ("_",)
        elif not identifiers:
            clean_exit("Error: at least one identifier is required.")
        LOGGER.info("Running program: %s", progname)

        # remove parameters that were not given
        parameters = {
            key: value for key, value in parameters.items() if value is not None
        }

        # parse tasks
        tasks, attachments = parse_tasks(
            identifiers,
            output_indices,
            output_branches,
            parameters=parameters,
            program=progname,
            attach=attach,
        )

        # run tasks
        opts = {
            "mode": mode,
            "overwrite": overwrite,
            "dry": dry,
            "no_fallback": no_fallback,
            "autorun": autorun,
        }
        tasks = run_tasks(tasks, attachments, **opts)

    return program


# convert machines parameter types to click types
PARAM_TYPES = {
    parameters.STRING: click.STRING,
    parameters.BOOL: click.BOOL,
    parameters.INT: click.INT,
    parameters.FLOAT: click.FLOAT,
}


def toolbox_options(toolbox):
    def decorator(func):
        kwargs = {"cls": OptionWithConfig}
        for init in toolbox.initializers:
            for name, parameter in init.parameters.items():
                setup_parameter(func, name, parameter, **kwargs)
        return func

    return decorator


def program_options(program):
    """set parameter/option for program cli"""

    def decorator(func):
        kwargs = {"section": "Program Options", "cls": OptionWithConfig}
        for name, parameter in program.parameters.items():
            setup_parameter(func, name, parameter, **kwargs)
        return func

    return decorator


def format_option(name):
    """format (long) option name for cli"""
    return "--" + name.replace("_", "-")


def format_help(help, **values):
    help = help.strip(".")
    if not values:
        return f"{help}."
    # else
    values = "[" + ", ".join(f"{key}={value}" for key, value in values.items()) + "]"
    return f"{help} {values}."


def setup_parameter(func, name, parameter, **kwargs):
    """add machine Parameter to click command"""
    args = kwargs

    # declarations (no short names)
    dest = parameter.name if parameter.name else name
    long_name = format_option(dest)
    values = {}

    # type
    if isinstance(parameter.type, parameters.Choice):
        value_type = click.Choice(parameter.type.values)
    else:
        value_type = PARAM_TYPES.get(parameter.type, click.STRING)

    # nargs
    if parameter.nargs is not None:
        args["type"] = MultiParameter(parameter.nargs, value_type)
    else:
        args["type"] = value_type

    # help
    help = parameter.help if parameter.help else str(parameter)
    if not parameter.required:
        values["default"] = str(parameter.default)
    args["show_choices"] = True
    args["help"] = format_help(help, **values)

    # set option
    flags = parameter.flags or []
    flagargs = {}
    for flag in flags:
        flag_name = format_option(flag)
        flag_value = flags[flag]
        flagargs["flag_value"] = flag_value
        if len(flags) != 1:
            flagargs["help"] = format_help(f"Set `{name}` to `{flag_value}`")
        click.option(flag_name, dest, **{**args, **flagargs})(func)

    if len(flags) != 1:
        click.option(long_name, dest, **args)(func)
    return


def program_manual(manual):
    """add manual to click. command"""

    def decorator(func):
        def show_manual(ctx, name, value):
            if not value:
                return
            if callable(manual):
                click.echo(manual())
            else:
                click.echo(manual)
            clean_exit()

        click.option(
            "--manual",
            is_flag=True,
            is_eager=True,
            callback=show_manual,
            help="Show manual for this program",
        )(func)
        return func

    return decorator


# utils


def utils_info():
    """Info utilities"""

    @click.command()
    @click.option("-a", "--all", is_flag=True, help="Desplay more details.")
    def info(all):
        """Display info on current toolbox."""
        session = ctxget("session")
        click.echo(f"machines version: {__version__}")
        click.echo(f"toolbox name: {session.toolbox.name}")

        main_storage = session.factory.main_storage
        temp_storage = session.factory.temp_storage
        dedicated = {
            storage
            for storage in session.storages
            if not storage in {main_storage, temp_storage}
        }

        if not all:
            click.echo(f"main storage: '{Path(main_storage.memory.root)}'")
            click.echo(f"temp storage: '{Path(temp_storage.memory.root)}'")
            if dedicated:
                other = [f"'{Path(storage.memory.root)}'" for storage in dedicated]
                click.echo(f"dedicated storages: {', '.join(other)}")
        else:
            click.echo(f"main storage: '{main_storage.memory.converter}'")
            click.echo(f"temp storage: '{temp_storage.memory.converter}'")
            if dedicated:
                click.echo(f"dedicated storages:")
                for storage in dedicated:
                    click.echo(f"\t{storage.name}: {storage.memory.converter}")

            click.echo("targets:")
            for name in sorted(session.factory.storages):
                if name.startswith("__"):
                    continue
                storage = session.factory.storages[name]
                click.echo(f"\t{name}: {storage.name}")

    return info


def utils_summary():
    """Summary utility"""

    @click.command()
    @click.argument("expr", metavar="TARGETS", required=False, nargs=-1)
    @click.option(
        "-n",
        "--maxnum",
        default=20,
        help="Show only first n results per storage location",
    )
    @click.option(
        "--temp",
        is_flag=True,
        help="Show temporary storages",
    )
    @click.option(
        "-s", "--storage", "storages", multiple=True, help="Filter by storage name."
    )
    @click.option("-o", "--output", help="Store summary to output file (.csv).")
    @click.option("--invalid", is_flag=True, help="List invalid targerts.")
    @click.option(
        "--rel-path",
        "path",
        flag_value="relpath",
        help="Display target's relative path",
    )
    @click.option(
        "--abs-path",
        "path",
        flag_value="abspath",
        help="Display target's absolute path",
    )
    @click.option(
        "--user-path",
        "path",
        flag_value="userpath",
        help="Display target's relative path to home",
    )
    def summary(expr, storages, maxnum, path, temp, output, invalid):
        """List existing targets."""
        if not expr:
            # if not provided
            expr = ["."]

        session = ctxget("session")

        summary = {}
        for storage in session.storages:
            if not temp and storage.temporary:
                continue
            elif storages and not any(name in storage.name for name in storages):
                # filter by storage name
                continue

            # invalid
            if invalid:
                pathes = storage.failed()
                if pathes:
                    summary[storage] = pathes
                continue

            # list target in storage
            parser = IndexParser(storage)
            targets = parser.targets(expr, click.echo)
            if not targets:
                continue
            summary[storage] = targets

        if invalid:
            # display invalid targets
            if not summary:
                clean_exit("No parsing error was found.")
            for storage in summary:
                pathes = summary[storage]
                click.echo(f"{storage} [{len(pathes)}], invalid pathes:")
                for path in pathes:
                    click.echo(f"\t{path}")
            clean_exit()

        if not summary:
            clean_exit(f"No maching target was found.")

        output_rows = []
        for storage in sorted(summary, key=lambda storage: storage.name):
            targets = summary[storage]
            click.echo(f"{storage} [{len(targets)}]")
            for target in sorted(targets)[:maxnum]:
                if path is None:
                    res = str(target)
                elif path == "abspath":
                    res = Path(storage.location(target)).resolve()
                elif path == "userpath":
                    res = (
                        Path(storage.location(target))
                        .resolve()
                        .relative_to(Path.home())
                    )
                elif path == "relpath":
                    res = storage.location(target)

                click.echo(f"\t{res}")
            if len(targets) > maxnum:
                click.echo(f"\t... ({len(targets) - maxnum} remaining)")

            if not output:
                continue
            for target in sorted(targets):
                output_rows.append(
                    [
                        utils.id_to_string(target.index.values),
                        utils.id_to_string(target.branch.values, none=""),
                        target.name,
                        storage.name,
                    ]
                )

        # output file
        if output:
            output = Path(output).with_suffix(".csv")
            if output.is_file():
                clean_exit(f"Output file already exists: {output}")
            elif not output.parent.exists():
                clean_exit(f"Invalid parent directory: {output.parent}")
            output_rows = [["index", "branch", "name", "storage"]] + output_rows
            with open(output, "w") as fp:
                writer = csv.writer(fp, delimiter=";", lineterminator="\n")
                writer.writerows(output_rows)

    return summary


def utils_export():
    """Export utility"""

    @click.command()
    @click.argument("expr", metavar="TARGETS", required=True, nargs=-1)
    @click.option("--dest", default="export", help="Export directory")
    @click.option(
        "-t",
        "--targetdir",
        type=KeyValue(
            [
                "!name",
                "struct",
                "index",
                "branch",
                "noindex",
                "nobranch",
                "default_branch",
            ],
        ),
        help="Setup target directory syntax (keyword 'name' must be set to the selected target).",
    )
    @click.option(
        "-s", "--storage", "storages", multiple=True, help="Filter by storage name."
    )
    @click.option(
        "-o",
        "--overwrite",
        is_flag=True,
        help="Overwrite existing destination",
    )
    def export(expr, dest, targetdir, storages, overwrite):
        """Export some targets."""
        session = ctxget("session")
        toolbox = ctxget("toolbox")

        if not targetdir:
            targetdir = {}
        target_name = targetdir.get("name")

        # create new storage
        targetdir["path"] = dest
        new_storage = setup_storage(targetdir, toolbox=toolbox)

        summary = {}
        errors = {}
        click.echo("Exporting targets:")
        for storage in session.storages:
            if storage.temporary:
                continue
            elif storages and not any(name in storage.name for name in storages):
                # filter by storage name
                continue
            # list target in storage
            parser = IndexParser(storage)
            for string in expr:
                for target in parser.targets(string, click.echo):
                    if target_name and target.name != target_name:
                        continue
                    source = storage.location(target)
                    dest = Path(new_storage.location(target)).resolve()
                    if dest.exists():
                        if not overwrite:
                            msg = f"Target already exists in destination."
                            errors[target] = msg
                            continue
                        new_storage.remove(target)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        shutil.copytree(source, dest)
                    except Exception as exc:
                        msg = f"An error occured: {exc}"
                        errors[target] = msg
                    else:
                        click.echo(f"\t{target}")
                        summary[target] = dest

        if errors:
            click.echo("The following targets were not exported:")
            for target, msg in errors.items():
                click.echo(f"{target}: {msg}")

        if not summary:
            click.echo("Nothing was done.")
        else:
            click.echo(f"{len(summary)} targets were exported.")

    return export


def utils_view():
    """View utility"""

    @click.command()
    @click.argument("expr", metavar="TARGETS", required=False, default=".")
    @click.option("--auto", help="Auto-open file in target directory.", is_flag=True)
    @click.option(
        "-f", "--file", "filename", default="*", help="Open file (wildcards accepted)."
    )
    @click.option(
        "--temp",
        is_flag=True,
        help="Show temporary storages",
    )
    @click.option(
        "-s", "--storage", "storages", multiple=True, help="Filter by storage name."
    )
    def view(expr, auto, filename, storages, temp):
        """View targets (open in directory system)."""
        session = ctxget("session")
        summary = {}
        for storage in session.storages:
            if not temp and storage.temporary:
                continue
            elif storages and not any(name in storage.name for name in storages):
                # filter by storage name
                continue
            # list target in storage
            parser = IndexParser(storage)
            targets = parser.targets(expr, click.echo)
            if not targets:
                continue
            summary[storage] = targets

        if not summary:
            clean_exit("No maching target was found.")

        def _launch(path):
            # click.launch(str(path), locate=...)
            return os.startfile(path)

        for storage, targets in summary.items():
            if not targets:
                continue
            tot = len(targets)
            click.echo(f"In {storage}:")
            for i, target in enumerate(targets):
                # get target path
                path = Path(storage.location(target))
                if auto:
                    glob = list(path.glob(filename))
                    if glob:
                        path = glob[0]  # use first match

                click.echo(f"\t{target} ({i+1}/{tot})")
                _launch(path)
                while True:
                    ans = click.prompt("\t(c)ontinue/(s)how again/(p)arent/(q)uit")
                    if ans == "c":
                        break
                    elif ans == "q":
                        clean_exit()
                    elif ans == "p":
                        _launch(path.parent)
                    elif ans == "s":
                        _launch(path)

    return view


def utils_cleanup():
    """Cleanup tempdir"""

    @click.command()
    @click.option("-y", "--yes", "silent", is_flag=True, help="Silent confirmation.")
    def cleanup(silent):
        """Cleanup temporary directory"""
        session = ctxget("session")

        # ask for confirmation
        if not silent:
            # list temp storages

            summary = {}
            for storage in session.storages:
                if not storage.temporary:
                    continue
                targets = storage.list()
                if not targets:
                    continue
                summary[storage] = targets
            if not summary:
                clean_exit("Nothing to do.")
            click.echo("These temporary targets will be removed:")
            for storage in summary:
                click.echo(f"In {storage}:")
                for target in summary[storage]:
                    click.echo("\t" + str(target))
            confirm = click.confirm("Are you sure? (yes/no)", default=False)
            if not confirm:
                clean_exit("Aborting.")

        targets = session.cleanup()
        if not targets:
            click.echo("Nothing to do.")
            return
        click.echo(f"Cleaned-up {len(targets)} targets")

    return cleanup


def utils_remove():
    """Remove utility"""

    @click.command()
    @click.argument("expr", metavar="TARGETS", required=True)
    @click.option("-y", "--yes", "silent", is_flag=True, help="Silent confirmation.")
    @click.option(
        "-s", "--storage", "storages", multiple=True, help="Filter by storage name."
    )
    def remove(expr, storages, silent):
        """Remove existing targets."""
        session = ctxget("session")
        summary = {}
        for storage in session.storages:
            if storages and not any(name in storage.name for name in storages):
                # filter by storage name
                continue
            # list target in storage
            parser = IndexParser(storage)
            targets = parser.targets(expr, click.echo)
            if not targets:
                continue
            summary[storage] = targets

        if not summary:
            clean_exit("No maching target was found.")

        click.echo("The following targets will be removed:")
        for storage, targets in summary.items():
            if not targets:
                continue
            click.echo(f"{storage} [{len(targets)}]")
            for target in targets:
                click.echo(f"\t{target}")

        # ask for confirmation
        if not silent:
            confirm = click.confirm("Are you sure? (yes/no)", default=False)
            if not confirm:
                clean_exit("No target were removed. Exiting.")

        for storage, targets in summary.items():
            for target in targets:
                try:
                    storage.remove(target)
                except TargetIsLocked as exc:
                    click.echo(f"Target locked: {target}, skipping.")
                except Exception as exc:
                    click.echo(f"An error occured when removing {target}: {exc}")
        click.echo("Done.")

    return remove


BATCH_EPILOG = """
Additional options for batch files:

\b
Configuration 'header' (put at the top of the batch file)
'''
    CONFIG:
      ALIAS:
        alias: [prog1, prog2]
      PARAMETERS:
        # global parameter values
        param1: value1
        prog1:
          param2: value2
      PATH:
        # path options
        PREFIX: <path-prefix>
'''
\b

"""


def utils_batch():
    """Run tasks in batch file."""

    @click.command(short_help="Run commands in batch file.", epilog=BATCH_EPILOG)
    @click.argument("file", type=click.Path(exists=True))
    @click.argument("identifiers", nargs=-1)
    @click.option("--branch", help="Output branch.")
    @click.option(
        "-o", "--overwrite", is_flag=True, help="Set write mode to 'overwrite'."
    )
    @click.option(
        "--mode",
        type=click.Choice(["overwrite", "upgrade", "upgrade"]),
        help="Write mode for existing data.",
    )
    @click.option("--dry", is_flag=True, help="Dry run, print dependency graph.")
    @click.option("--no-fallback", is_flag=True, help="Disable branch fallback.")
    def batch(file, identifiers, overwrite, mode, dry, branch, no_fallback):
        """Run commands in batch file FILE.

        \b
        id[~branch]: # output index
            program:
                index: ... # input index (if different from output index)
                param1: ...
            param2: ...  # common parameter
        etc.

        """
        # parse tasks
        opts = {
            "mode": mode,
            "overwrite": overwrite,
            "dry": dry,
            "no_fallback": no_fallback,
        }
        tasks, attachments = parse_tasks(
            [file] + list(identifiers),
            output_indices=None,
            output_branches=branch,
        )

        # run tasks
        run_tasks(tasks, attachments, **opts)

    return batch


def run_tasks(
    tasks,
    attachments,
    mode=None,
    overwrite=False,
    dry=False,
    no_fallback=False,
    autorun=False,
):
    """run several tasks"""

    if not tasks:
        clean_exit("Nothing to do.")

    session = ctxget("session")
    global_options = ctxget("options")

    if autorun:
        session_func = session.autorun
    else:
        session_func = session.run

    # make attachment callback
    callback_attach = make_callback_attach(attachments)

    # common options
    options = dict(
        global_options,
        mode=mode,
        overwrite=overwrite,
        dry=dry,
        show_all=dry,
        hold=False,
        fallback=not no_fallback,
        callback=[callback_attach, callback_printer(**global_options)],
    )

    # run tasks
    summary = []
    for task in tasks:
        # run all programs in batch
        try:
            _summary = session_func(
                task["program"],
                indices=task["input_indices"],
                branches=task["input_branches"],
                output_indices=task["output_indices"],
                output_branches=task["output_branches"],
                parameters=task["parameters"],
                meta={"program": task["program"]},
                **options,
            )
            summary.extend(_summary)
        except (InvalidTarget, RejectException, ExpectedError, ParameterError) as exc:
            click.echo(f"<{exc.error}> {exc}")
        except Exception as exc:
            click.echo(f"An error occured: {repr(exc)}")
            LOGGER.info(traceback.format_exc())

    if dry and summary:
        # print tasks
        click.echo("Tasks:")
        for task in summary:
            if not task.temporary:
                click.echo(f"\t{task}")

    # hold session until all tasks are completed
    session.hold()

    for task in session.monitor(show_all=options.get("show_all")):
        if task.status.name == "PENDING":
            printer_pending(task)

    # list temporary
    ndir, ntemp = 0, 0
    for storage in session.storages:
        if not storage.temporary:
            continue
        ndir += 1
        ntemp += len(storage.list())
    if ntemp:
        click.echo(
            f"There are {ntemp} unfinished tasks in {ndir} temporary directorie(s)."
        )


def make_callback_attach(attachments):
    """attach info to input targets"""

    def callback_attach(task, msg=None, attachments=attachments):
        if task.status.name != "RUNNING":
            return
        elif not attachments:
            return
        # set target's attachments
        for input in task.flat_inputs:
            if input in attachments:
                input.attach(attachments[input])

        # also attach metadata to task.meta
        if task.identifier in attachments:
            task.meta.update(attachments[task.identifier])

    return callback_attach


#
# printer


class callback_printer:
    def __init__(self, **options):
        self.options = options
        self.running = {}

    def __call__(self, task, msg=None):
        """print info after process is complete"""

        def print_error(exc):
            if not exc:
                return ""
            elif not isinstance(exc, Exception):
                return str(exc)
            name = type(exc).__name__
            return f"<{name}>{exc}"

        status = task.status.name
        output = str(task.output) if task.output else "(no output)"
        taskstr = f"{str(task.index)}~{str(task.branch)}"
        program = task.meta["program"]
        taskid = (program, taskstr)
        msg = print_error(msg)

        show_all = self.options["show_all"]

        if status == "RUNNING":
            if show_all:
                click.echo(f"Task: {program} -> {output} is running.")
            elif not taskid in self.running.get(program, []):
                click.echo(f"Task: {program} is running for {taskstr}")
                self.running.setdefault(program, []).append(taskid)
            # else: ignore

        elif status == "REJECTED":
            click.echo(f"Task: {program} -> {output} was rejected: {msg}.")

        elif status == "ERROR":
            click.echo(f"Task: {program} -> {output} had an error: {msg}.")

        elif show_all or not task.temporary:
            # only show if task is not temporary
            if status == "SKIPPED":
                click.echo(f"Task: {program} -> {output} was skipped.")

            elif status == "SUCCESS":
                click.echo(f"Task: {program} -> {output} is done.")

        if msg:
            LOGGER.info(msg)


def printer_pending(task):
    output = f" -> {task.output}" if task.output else ""
    program = task.meta["program"]
    missing = ", ".join(
        [name for name, targets in task.available_inputs.items() if not targets]
    )
    click.echo(f"Task: {program}{output} is pending (missing inputs: {missing})")


# parsers


def parse_tasks(
    identifiers,
    output_indices=None,
    output_branches=None,
    program=None,
    parameters=None,
    attach=[],
):
    """parse tasks from list of identifiers or batch files"""

    session = ctxget("session")
    parser = IndexParser(*session.storages)

    batch_tasks = []
    cmd_tasks = []
    attachments = {}

    if not parameters:
        parameters = {}

    # parse output ids / branches
    if output_indices:
        output_indices = parser.indices(output_indices)
    if output_branches:
        output_branches = parser.branches(output_branches)

    has_cmd = False
    for item in identifiers:
        try:
            is_file = Path(item).is_file()
        except OSError:
            is_file = False

        if is_file:
            # argument is a file: load as batch
            programs = ctxget("aliases")
            try:
                _tasks, _attachments = parse_batch(
                    item, parser, programs=programs, new_branches=output_branches
                )
            except (BatchFileError, IndexParserError) as exc:
                clean_exit(exc)

            # store attachments
            attachments.update(_attachments)

            # update parameters
            [task["parameters"].update(parameters) for task in _tasks]

            if program:
                # if program is given, only keep matching tasks
                batch_tasks.extend(
                    [task for task in _tasks if task["program"] == program]
                )
            else:
                # run tasks for all programs
                batch_tasks.extend(_tasks)

        else:
            has_cmd = True
            # parse argument as string identifiers
            identifiers = parser.identifiers(item, click.echo)
            if not identifiers:
                continue
            cmd_tasks.append(
                {
                    "program": program,
                    # "task": f"{item}",
                    "identifiers": identifiers,
                    "input_indices": [id.index for id in identifiers],
                    "input_branches": [id.branch for id in identifiers],
                    "output_indices": output_indices,
                    "output_branches": output_branches,
                    "parameters": parameters,
                }
            )

    if batch_tasks and has_cmd:
        # if both batch and cli tasks: keep subset of batch tasks
        tasks = []
        for task in cmd_tasks:
            for btask in list(batch_tasks):
                subset = set(btask["identifiers"]) & set(task["identifiers"])
                if subset:
                    task_ = btask.copy()
                    # task_["parameters"] = {**btask["parameters"], **task["parameters"]}
                    task_["identifiers"] = list(subset)
                    if not btask["output_indices"]:
                        task_["input_indices"] = [id.index for id in subset]
                        task_["input_branches"] = [id.branch for id in subset]
                    tasks.append(task_)

    elif cmd_tasks:
        # only CLI tasks
        tasks = cmd_tasks

    else:
        # only batch tasks
        tasks = batch_tasks

    # update missing parameters using common parameters
    if parameters:
        for task in tasks:
            task["parameters"].update(
                {
                    key: parameters[key]
                    for key in parameters
                    if not key in task["parameters"]
                }
            )

    # command-line attach
    for strids, value in attach:
        for target in parser.targets(strids, click.echo):
            attachments[target] = value

    return tasks, attachments


#
#
# click subclasses


class MultiParameter(click.ParamType):
    """Undetermined-number parameter
    expects a comma-separated list: "--param value1,value2,..."

    """

    name = "values"

    def __init__(self, nargs=-1, type=click.STRING, sep=","):
        self._nargs = nargs
        self._type = type
        self._sep = sep
        super().__init__()

    def convert(self, value, param, ctx):
        """convert multi-valued parameter"""
        if not value:
            return self.fail("At least one value must be provided")

        # split into values
        if isinstance(value, str):
            value = value.split(self._sep)
        return [self._type(item, param, ctx) for item in value]


class Separators(click.ParamType):
    """parameter type for separators"""

    name = "separators"

    def convert(self, value, param, ctx):
        if not len(value) == 4:
            self.fail("There must be 4 separator characters")
        return value


class YAMLString(click.ParamType):
    name = "yaml"

    def convert(self, value, param, ctx):
        item = yaml.safe_load(value)
        assert len(item) == 1
        key = list(item)[0]
        value = item[key]
        return key, value


class KeyValue(click.ParamType):
    """
    Dict-like parameter type.
    Values are passed as:
        "key1=value1,key3=value3,etc."
    or
        "value1::value3:etc."
    """

    name = "value1:value2:..."

    def __init__(self, keys, types=str, *args, **kwargs):
        # parse keys
        if not isinstance(keys, (tuple, list)):
            keys = [keys]
        if not all(isinstance(key, str) for key in keys):
            raise TypeError(f"Invalid keys: {keys}")

        self._keys = [key.replace("!", "") for key in keys]
        self._required_keys = [key.replace("!", "") for key in keys if "!" in key]
        if isinstance(types, type):
            types = [types] * len(keys)
        self._types = dict(zip(self._keys, types))
        super().__init__(*args, **kwargs)

    def convert(self, value, param, ctx):
        """parse value into dict"""
        if isinstance(value, dict):
            values = value

        elif {":", "="} <= set(value):
            self.fail("Cannot have both ':' and '='")

        elif "=" in value:
            if "," in value:
                click.echo(
                    f"Warning: using comma ',' in '{value}' is deprecated. Use `;` instead"
                )
                split = value.split(",")
            else:
                split = value.split(";")

            values = {}
            for item in split:
                key, val = item.split("=")
                if not key in self._keys:
                    self.fail(f"Unknown key: {key}")
                values[key] = self._types[key](val)

        else:  # ":" in value or None:
            split = value.split(":")
            values = {
                key: self._types[key](item)
                for key, item in zip(self._keys, split)
                if item
            }

        missing = set(self._required_keys) - set(values)
        if missing:
            self.fail(f"Missing required keys: {missing}")

        return values


# config


class OptionWithConfig(click.Option):
    """click.Option with config file"""

    def __init__(self, *args, section=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.section = section

    def consume_value(self, ctx, opts):
        config = ctxget("config", {})
        if opts.get(self.name) is None and self.name in config:
            opts[self.name] = config[self.name]
        return super().consume_value(ctx, opts)


def callback_default_toolbox(ctx, param, value):
    """force use of default toolbox"""
    ctxset("toolbox_default", value)


def callback_config(ctx, param, value):
    """on callback load config and store in context"""
    path = Path(value)
    if path.is_file():
        with open(path) as fp:
            try:
                config = yaml.safe_load(fp)
            except Exception as exc:
                clean_exit(f"Invalid config file ({path}): {exc}")

        # use a different toolbox
        toolbox_default = ctxget("toolbox_default", False)
        if not toolbox_default and "toolbox" in config:
            # load toolbox from config file
            module, toolbox = load_toolbox(config.pop("toolbox"))
            if (toolbox is not ctxget("toolbox")) or (module is not ctxget("module")):
                # use only if different from current toolbox and module
                click.echo(f"Using toolbox: {toolbox.name}")
                cli = setup(toolbox, module=module)
                # startover parsing with new toolbox
                cli.main()

        click.echo(f"Using configuration file: {path}")
        ctxset("config", config)


def load_toolbox(string):
    """load toolbox from path:name or module:name string"""
    import importlib

    module_name, toolbox_name = string.split(":")
    if Path(module_name).is_file():
        module_path = Path(module_name)
        if ctxget("module") is None or module_path != Path(ctxget("module").__file__):
            name = Path(module_path).stem.replace("-", "_")
            spec = importlib.util.spec_from_file_location(name, module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            module = ctxget("module")
    else:
        module = importlib.import_module(module_name)
    toolbox = getattr(module, toolbox_name)
    return module, toolbox


# custom groupsep


class OrderedGroup(click.Group):
    """click Group with ordered commands"""

    def __init__(self, name=None, commands=None, **attrs):
        super().__init__(name, commands, **attrs)
        # commands are ordered
        self.commands = commands or collections.OrderedDict()

    def list_commands(self, ctx):
        return self.commands


class Section:
    """Define a section for a section group"""

    def __init__(self, name, group, help=None):
        if not isinstance(group, SectionGroup):
            raise TypeError("Only SectionGroup can be used with Section objects")
        self.name = name
        self.group = group
        self.help = help
        self.commands = []

    def command(self, name=None, cls=None, **attrs):
        """decorator for adding a click.Command"""
        return self.group.command(name, cls, section=self, **attrs)

    def group(self, name=None, **attrs):
        """decorator for adding a click.Group"""
        return self.group.command(name, click.Group, section=self, **attrs)


class SectionGroup(OrderedGroup):
    """click Group with commands put into sections"""

    def __init__(self, name=None, commands=None, **attrs):
        super().__init__(name=name, commands=None, **attrs)
        # commands are ordered
        self.commands = commands or collections.OrderedDict()
        # sections
        self.sections = {}
        self.default_section = Section("Other commands", self)

    def add_command(self, cmd, name=None, section=None):
        super().add_command(cmd, name)
        if not section:
            section = self.default_section
        elif not isinstance(section, Section):
            raise TypeError("section must be a Section object")
        section.commands.append(cmd)

    def command(self, *args, **kwargs):
        """decorator for adding a click.Command"""
        section = kwargs.pop("section", None)

        def decorator(func):
            cmd = click.command(*args, **kwargs)(func)
            self.add_command(cmd, section=section)
            return cmd

        return decorator

    def group(self, *args, **kwargs):
        """same as self.command for click.Group"""
        section = kwargs.pop("section", None)

        def decorator(func):
            cmd = click.group(*args, **kwargs)(func)
            self.add_command(cmd, section=section)
            return cmd

        return decorator

    def add_section(self, name, help=None):
        """create new section"""
        if name is None:
            # return default section
            return self.default_section

        if name in self.sections:
            raise ValueError(f"Session '{name}' already exists")
        section = Section(name, self, help)
        self.sections[name] = section
        return section

    def section(self, name=None, help=None):
        """decorator for creating a new section"""

        def decorator(func):
            # nonlocal name, help
            if not name:
                name = func.__name__
            if not help:
                help = func.__doc__
            section = self.add_section(name, help)
            return section

        return decorator

    def format_commands(self, ctx, formatter):
        """for format group commands"""
        if not self.sections:
            return super().format_commands(ctx, formatter)

        with formatter.section("Commands"):

            commands = self.list_commands(ctx)
            limit = formatter.width - 6 - max(len(cmd[0]) for cmd in commands)
            for name, section in self.sections.items():
                if not section.commands:
                    continue
                with formatter.section(name):
                    self.format_section(section, formatter, limit)

            if self.default_section.commands:
                with formatter.section(self.default_section.name):
                    self.format_section(self.default_section, formatter, limit)

    def format_section(self, section, formatter, limit=None):
        """format section"""
        if section.help:
            with formatter.indentation():
                formatter.write_text(section.help)
                formatter.write_paragraph()
        helpers = [
            (cmd.name, cmd.get_short_help_str(limit)) for cmd in section.commands
        ]
        if helpers:
            formatter.write_dl(helpers)


class CommandWithSections(click.Command):
    """click command with parameter sections"""

    def format_options(self, ctx, formatter):
        """Writes all the options into the formatter if they exist."""
        opts = collections.OrderedDict()
        for param in self.get_params(ctx):
            rv = param.get_help_record(ctx)
            if rv is not None:
                section = getattr(param, "section", None)
                opts.setdefault(section, []).append(rv)

        if opts.get(None):
            with formatter.section(click.core._("Options")):
                formatter.write_dl(opts.pop(None))
        for section in opts:
            with formatter.section(click.core._(section)):
                formatter.write_dl(opts[section])


def clean_exit(msg=None):
    """clean exiting"""
    if msg:
        click.echo(msg)
    raise SystemExit()
