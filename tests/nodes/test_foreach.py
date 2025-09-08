from json import dumps, loads

import pytest

from argus import Foreach
from argus.utils import additem, addy, double, getitems, triple


def test_foreach_with_function(tmp_path):
    """Test that Foreach run as expected gived a function"""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 2}))

    node = Foreach(getitems).then(double)
    node.run()

    data_path = tmp_path / ".argus" / "data.json"
    result = loads(data_path.read_text())

    assert "x" in result
    assert isinstance(result["x"], int)
    assert result["x"] == 4


def test_foreach_with_list(tmp_path):
    """Test that Foreach runs as expected given a list"""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 5}))

    node = Foreach(["a", "b"]).then(double)
    node.run()

    data_path = tmp_path / ".argus" / "data.json"
    result = loads(data_path.read_text())
    assert "x" in result
    assert isinstance(result["x"], int)
    assert result["x"] == 10


def test_foreach_with_item(tmp_path):
    """Test that Foreach runs as expected when using the items in the given list."""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 5}))

    node = Foreach([1, 2]).then(additem)
    node.run()

    data_path = tmp_path / ".argus" / "data.json"
    result = loads(data_path.read_text())
    assert "x" in result
    assert isinstance(result["x"], list)
    assert sorted(result["x"]) == [6, 7]


def test_foreach_with_named_item(tmp_path):
    """Test that Foreach runs as expected when using named items in the given list."""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 1}))

    node = Foreach([2, 5], item_name="y").then(addy)
    node.run()

    data_path = tmp_path / ".argus" / "data.json"
    result = loads(data_path.read_text())
    assert "x" in result
    assert isinstance(result["x"], list)
    assert sorted(result["x"]) == [3, 6]
    assert sorted(result["y"]) == [2, 5]


def test_foreach_to_argo_function():
    """Test that Foreach.to_argo produces the expected structure given a function."""
    node = Foreach(getitems).then(double)
    steps, templates = node.to_argo(2)

    assert isinstance(steps, list)
    assert any("foreachmerge" in t.name for t in templates)


def test_foreach_to_argo_list():
    """Test that Foreach.to_argo produces the expected structure given a list."""
    node = Foreach(["a", "b"]).then(double)
    steps, templates = node.to_argo(3)

    assert isinstance(steps, list)
    assert any("foreachmerge" in t.name for t in templates)


def test_foreach_then_twice_raises():
    """Test that wrong order fails."""
    node = Foreach(getitems).then(double)
    with pytest.raises(RuntimeError, match="must follow Foreach"):
        node.then(triple)
