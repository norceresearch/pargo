from __future__ import annotations

from copy import deepcopy
from importlib import import_module
from inspect import signature
from json import dumps, loads
from os import environ
from pathlib import Path
from typing import Any

from loguru import logger


def run(
    task_name: str,
    module_name: str,
    data: dict[str, Any] | None = None,
    item: dict[str, Any] = {},
):
    logger.info(f"Running task {task_name} from {module_name}")
    module = import_module(module_name)
    func = getattr(module, task_name)

    sig = signature(func)
    inputs = {k: v for k, v in {**data, **item}.items() if k in sig.parameters}
    result = func(**inputs)
    return result


def load_item():
    item = loads(environ.pop("PARGO_ITEM", "{}"))
    return {k: loads(v) for k, v in item.items()}


def pargo_path():
    pargo_path = Path(environ.get("PARGO_DIR", Path.cwd() / ".pargo"))
    pargo_path.mkdir(exist_ok=True, parents=True)
    return pargo_path


def run_step(
    task_name: str,
    module_name: str,
    data: dict[str, Any] | None = None,
    item: dict[str, Any] = {},
):
    remote = True if data is None else False
    if remote:
        data = loads(environ.pop("PARGO_DATA"))
        item = load_item()

    result = run(task_name, module_name, data, item)
    result = {} if result is None else result
    if not isinstance(result, dict):
        raise ValueError(
            f"Task `{task_name}` must return a dict or None, got {type(result).__name__}"
        )
    data = deepcopy(data)
    data.update(data)
    logger.info(f"Data passed to next step: {dumps(data)}")
    if remote:
        data_path = pargo_path() / "data.json"
        data_path.write_text(dumps(data))
    return data


def run_when(task_name: str, module_name: str, data: dict[str, Any] | None = None):
    remote = True if data is None else False
    if remote:
        data = loads(environ.pop("PARGO_DATA"))
    result = run(task_name, module_name, data)

    if not isinstance(result, bool):
        raise ValueError(
            f"Condition `{task_name}` must return bool, got {type(result).__name__}"
        )

    if remote:
        when_path = pargo_path() / "when.json"
        when_path.write_text(dumps(result))
    return result


def run_foreach(task_name: str, module_name: str, data: dict[str, Any] | None = None):
    remote = True if data is None else False
    if remote:
        data = loads(environ.pop("PARGO_DATA"))
    result = run(task_name, module_name, data)

    if not isinstance(result, list):
        raise ValueError(
            f"Foreach `{task_name}` must return list, got {type(result).__name__}"
        )

    if remote:
        result = [dumps(r) for r in result]
        foreach_path = pargo_path() / "foreach.json"
        foreach_path.write_text(dumps(result))
    return result


def merge_foreach(data: list[dict[str, Any]] | None = None):
    remote = True if data is None else False
    if remote:
        data = loads(environ.pop("PARGO_DATA"))

    merged = {}
    for d in data:
        for k, v in d.items():
            merged.setdefault(k, []).append(v)

    for k, vals in merged.items():
        if all(v == vals[0] for v in vals):
            merged[k] = vals[0]

    if remote:
        data_path = pargo_path() / "data.json"
        data_path.write_text(dumps(merged))
    return merged
