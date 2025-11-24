from json import dumps, loads
from os import environ

import pytest

from pargo import utils as utils
from pargo.nodes.run import (
    merge_foreach,
    run_foreach,
    run_step,
    run_when,
)


def test_run_step(tmp_path):
    """Test that run_step produces the expected output."""
    (tmp_path / ".pargo" / "data.json").write_text(dumps({"x": 3}))
    environ["PARGO_DATA"] = dumps({"x": 3})

    result = run_step("double", utils.__name__)
    assert "x" in result and result["x"] == 6


@pytest.mark.parametrize("task", ["choice", "get_items"])
def test_run_step_task_with_invalid_return_type(tmp_path, task):
    """run_step should return None or dict[str, Any]. Test that it fails for invalid return types (bool, list)."""
    (tmp_path / ".pargo" / "data.json").write_text(dumps({"x": 3}))
    environ["PARGO_DATA"] = dumps({"x": 3})

    with pytest.raises(ValueError):
        run_step(task, utils.__name__)


def test_run_when(tmp_path):
    """Test that run_when produces boolean output."""
    data = {"x": 3}
    (tmp_path / ".pargo" / "data.json").write_text(dumps(data))
    environ["PARGO_DATA"] = dumps(data)
    res = run_when("choice", utils.__name__)
    assert isinstance(res, bool)
    assert (tmp_path / ".pargo" / "when.json").exists()


@pytest.mark.parametrize("task", ["double", "triple", "get_items"])
def test_run_when_task_with_invalid_return_type(tmp_path, task):
    """run_when should return a boolean. Test that it fails for invalid return types (dict, dict, list)."""
    (tmp_path / ".pargo" / "data.json").write_text(dumps({"x": 3}))
    environ["PARGO_DATA"] = dumps({"x": 3})

    with pytest.raises(ValueError):
        run_when(task, utils.__name__)


def test_run_foreach(tmp_path):
    """Test that run_foreach produces the expected output."""
    environ["PARGO_DATA"] = dumps({})
    res = run_foreach("get_items", utils.__name__)
    assert isinstance(res, list)
    assert (tmp_path / ".pargo" / "foreach.json").exists()


def test_merge_foreach(tmp_path):
    """Test that merge_foreach correctly merges data."""
    data = [{"x": 1, "y": 2}, {"x": 1, "y": 3}]
    environ["PARGO_DATA"] = dumps(data)
    merge_foreach()
    merged = loads((tmp_path / ".pargo" / "data.json").read_text())
    assert merged["x"] == 1
    assert isinstance(merged["y"], list)
    assert sorted(merged["y"]) == [2, 3]


@pytest.mark.parametrize("task", ["double", "triple", "choice"])
def test_run_foreach_task_with_invalid_return_type(tmp_path, task):
    """run_foreach should return a list. Test that it fails for invalid return types (dict, dict, bool)."""
    (tmp_path / ".pargo" / "data.json").write_text(dumps({"x": 3}))
    environ["PARGO_DATA"] = dumps({"x": 3})

    with pytest.raises(ValueError):
        run_foreach(task, utils.__name__)
