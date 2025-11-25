
from inspect import getsourcefile
from pathlib import Path


def import_path(func):
    """Get the import path of tasks defined in a running script."""
    file = getsourcefile(func)

    path = Path(file).resolve()

    dirname = path.parent
    parts = [path.stem]

    while True:
        if (dirname / "__init__.py").exists():
            parts.append(dirname.name)
            dirname = dirname.parent
        else:
            break
    return ".".join(reversed(parts))