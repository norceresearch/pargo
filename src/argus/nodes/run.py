from __future__ import annotations

from importlib import import_module
from inspect import signature
from json import dumps, loads
from os import environ
from pathlib import Path

from loguru import logger


def load_and_run(task_name: str, module_name: str):
    logger.info(f"Running task {task_name} from {module_name}")
    module = import_module(module_name)
    func = getattr(module, task_name)

    data = loads(environ.pop("ARGUS_DATA", "{}"))
    item = load_item()
    sig = signature(func)
    inputs = {k: v for k, v in {**data, **item}.items() if k in sig.parameters}
    result = func(**inputs)
    return result, data


def load_item():
    item = loads(environ.pop("ARGUS_ITEM", "{}"))
    return {k: loads(v) for k, v in item.items()}


def argus_path():
    argus_path = Path(environ.get("ARGUS_DIR", Path.cwd() / ".argus"))
    argus_path.mkdir(exist_ok=True)
    return argus_path


def run_step(task_name: str, module_name: str, write_data: bool = True):
    result, data = load_and_run(task_name, module_name)
    result = {} if result is None else result
    if not isinstance(result, dict):
        raise ValueError(
            f"Task `{task_name}` must return a dict or None, got {type(result).__name__}"
        )
    data.update(result)
    logger.info(f"Saving data: {dumps(data)}")
    if write_data:
        data_path = argus_path() / "data.json"
        data_path.write_text(dumps(data))
    return data


def run_when(task_name: str, module_name: str):
    result, _ = load_and_run(task_name, module_name)

    if not isinstance(result, bool):
        raise ValueError(
            f"Condition `{task_name}` must return bool, got {type(result).__name__}"
        )
    when_path = argus_path() / "when.json"
    when_path.write_text(dumps(result))
    return result


def run_foreach(task_name: str, module_name: str):
    result, _ = load_and_run(task_name, module_name)

    if not isinstance(result, list):
        raise ValueError(
            f"Foreach `{task_name}` must return list, got {type(result).__name__}"
        )
    result = [dumps(r) for r in result]
    foreach_path = argus_path() / "foreach.json"
    foreach_path.write_text(dumps(result))
    return result


def merge_foreach():
    items = loads(environ.pop("ARGUS_DATA", "{}"))

    merged = {}
    for d in items:
        for k, v in d.items():
            merged.setdefault(k, []).append(v)

    for k, vals in merged.items():
        if all(v == vals[0] for v in vals):
            merged[k] = vals[0]

    data_path = argus_path() / "data.json"
    data_path.write_text(dumps(merged))
