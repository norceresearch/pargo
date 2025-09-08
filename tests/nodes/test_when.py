from json import dumps, loads

import pytest

from argus import When
from argus.utils import choice, double, triple


def test_when_branch_then(tmp_path):
    """Test that then branch runs when expected."""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 3}))

    node = When(choice).then(double).otherwise(triple)
    node.run()

    data_path = tmp_path / ".argus" / "data.json"
    result = loads(data_path.read_text())
    assert result["x"] == 6


def test_when_branch_otherwise(tmp_path):
    """Test that otherwise branch runs when expected."""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 4}))

    node = When(choice).then(double).otherwise(triple)
    node.run()

    data_path = tmp_path / ".argus" / "data.json"
    result = loads(data_path.read_text())
    assert result["x"] == 12


def test_when_to_argo():
    """Test that When.to_argo prodeces expected structure."""
    node = When(choice).then(double).otherwise(triple)
    steps, templates = node.to_argo(1)

    assert isinstance(steps, list)
    assert any("whenmerge" in t.name for t in templates)


def test_when_otherwise_without_then_raises():
    """Test that wrong order fails."""
    with pytest.raises(RuntimeError, match="must follow then"):
        When(choice).otherwise(triple)


def test_when_then_twice_raises():
    """Test that wrong order fails."""
    node = When(choice).then(double)
    with pytest.raises(RuntimeError, match="must follow When"):
        node.then(triple)


def test_when_otherwise_twice_raises():
    """Test that wrong order fails."""
    node = When(choice).then(double).otherwise(triple)
    with pytest.raises(RuntimeError, match="must follow then"):
        node.otherwise(double)
