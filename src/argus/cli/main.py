from argparse import ArgumentParser
from json import JSONDecodeError, loads
from pathlib import Path

from argus import Workflow


def load_workflows(path: Path) -> dict[str, Workflow]:
    """Executes a Python file and returns Workflows."""

    module_globals: dict[str, object] = {}
    code = path.read_text()
    exec(compile(code, str(path), "exec"), module_globals)

    workflows = {w.name: w for w in module_globals.values() if isinstance(w, Workflow)}

    if not workflows:
        raise RuntimeError(f"No Workflow objects found in {path}")
    return workflows


def cli():
    parser = ArgumentParser(prog="argus", description="Argus CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a workflow locally")
    run_parser.add_argument(
        "path", type=Path, help="Path to a Python file defining a Workflow"
    )
    run_parser.add_argument(
        "--param",
        action="append",
        default=[],
        help="Override workflow parameters (format key=value). Can be repeated.",
    )
    run_parser.add_argument(
        "--name",
        help="Name of the workflow to run. Defaults to last workflow defined in file.",
    )

    gen_parser = subparsers.add_parser("generate", help="Generate YAML manifest(s)")
    gen_parser.add_argument(
        "path", type=Path, help="Path to a Python file defining a Workflow"
    )
    gen_parser.add_argument(
        "--outdir",
        type=Path,
        default=Path.cwd(),
        help="Output directory. Defaults to current working directory.",
    )
    gen_parser.add_argument(
        "--name",
        help="Name of the workflow to generate. Defaults to last workflow defined in file.",
    )

    args = parser.parse_args()

    workflows = load_workflows(args.path)
    if args.name:
        if args.name not in workflows:
            raise RuntimeError(f"No workflow named '{args.name}' in {args.path}")
        wf = workflows[args.name]
    else:
        wf = next(reversed(workflows.values()))

    if args.command == "run":
        params = {}
        for key_value in args.param:
            if "=" not in key_value:
                raise ValueError(f"Expected key=value, got {key_value}")
            key, val = key_value.split("=", 1)
            params[key] = _parse_value(val)
        wf.run(params)
    elif args.command == "generate":
        wf.to_yaml(args.outdir)


def _parse_value(val: str):
    try:
        return loads(val)
    except JSONDecodeError:
        return val
