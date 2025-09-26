from json import dumps, loads
from os import environ

import pytest

from argus import utils as utils
from argus.nodes.run import (
    merge_foreach,
    merge_when,
    run_foreach,
    run_init,
    run_step,
    run_when,
)


def test_run_step(tmp_path):
    """Test that run_step produces the expected output."""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 3}))
    environ["ARGUS_DATA"] = dumps({"x": 3})

    result = run_step("double", utils.__name__)
    assert "x" in result and result["x"] == 6


@pytest.mark.parametrize("task", ["choice", "get_items"])
def test_run_step_task_with_invalid_return_type(tmp_path, task):
    """run_step should return None or dict[str, Any]. Test that it fails for invalid return types (bool, list)."""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 3}))
    environ["ARGUS_DATA"] = dumps({"x": 3})

    with pytest.raises(ValueError):
        run_step(task, utils.__name__)


def test_run_when(tmp_path):
    """Test that run_when produces boolean output."""
    data = {"x": 3}
    (tmp_path / ".argus" / "data.json").write_text(dumps(data))
    environ["ARGUS_DATA"] = dumps(data)
    res = run_when("choice", utils.__name__)
    assert isinstance(res, bool)
    assert (tmp_path / ".argus" / "when.json").exists()


def test_merge_when_then(tmp_path):
    """Test that then-brach is used when expected."""
    environ["ARGUS_THEN"] = dumps({"x": 1})
    environ["ARGUS_OTHER"] = "{{path.to.prev.step}}"
    merge_when()
    merged = loads((tmp_path / ".argus" / "data.json").read_text())
    assert merged["x"] == 1


def test_merge_when_otherwise(tmp_path):
    """Test that otherwise-branch is used when expected."""
    environ["ARGUS_THEN"] = dumps({"x": 2})
    environ["ARGUS_OTHER"] = dumps({"x": 1})
    merge_when()
    merged = loads((tmp_path / ".argus" / "data.json").read_text())
    assert merged["x"] == 1


@pytest.mark.parametrize("task", ["double", "triple", "get_items"])
def test_run_when_task_with_invalid_return_type(tmp_path, task):
    """run_when should return a boolean. Test that it fails for invalid return types (dict, dict, list)."""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 3}))
    environ["ARGUS_DATA"] = dumps({"x": 3})

    with pytest.raises(ValueError):
        run_when(task, utils.__name__)


def test_run_foreach(tmp_path):
    """Test that run_foreach produces the expected output."""
    environ["ARGUS_DATA"] = dumps({})
    res = run_foreach("get_items", utils.__name__)
    assert isinstance(res, list)
    assert (tmp_path / ".argus" / "foreach.json").exists()


def test_merge_foreach(tmp_path):
    """Test that merge_foreach correctly merges data."""
    data = [{"x": 1, "y": 2}, {"x": 1, "y": 3}]
    environ["ARGUS_DATA"] = dumps(data)
    merge_foreach()
    merged = loads((tmp_path / ".argus" / "data.json").read_text())
    assert merged["x"] == 1
    assert isinstance(merged["y"], list)
    assert sorted(merged["y"]) == [2, 3]


@pytest.mark.parametrize("task", ["double", "triple", "choice"])
def test_run_foreach_task_with_invalid_return_type(tmp_path, task):
    """run_foreach should return a list. Test that it fails for invalid return types (dict, dict, bool)."""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 3}))
    environ["ARGUS_DATA"] = dumps({"x": 3})

    with pytest.raises(ValueError):
        run_foreach(task, utils.__name__)


def test_run_init(tmp_path):
    """Test that run_init produces the expected output."""
    environ["ARGUS_PARAM_0"] = dumps({"test0": 42})
    environ["ARGUS_PARAM_1"] = dumps({"test1": 4.2})
    environ["ARGUS_PARAM_2"] = dumps({"test2": "text"})
    environ["ARGUS_PARAM_3"] = dumps({"test3": True})
    run_init()
    data = loads((tmp_path / ".argus" / "data.json").read_text())
    assert data["test0"] == 42
    assert data["test1"] == 4.2
    assert data["test2"] == "text"
    assert data["test3"] is True
