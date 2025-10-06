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


def test_when_branch_then_only(tmp_path):
    """Test that then branch runs when expected."""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 3}))
    node = When(choice).then(double)
    node.run()
    data_path = tmp_path / ".argus" / "data.json"
    result = loads(data_path.read_text())
    assert result["x"] == 6

    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 4}))
    node = When(choice).then(double)
    node.run()
    data_path = tmp_path / ".argus" / "data.json"
    result = loads(data_path.read_text())
    assert result["x"] == 4


def test_when_get_templates():
    """Test that When.to_argo prodeces expected structure."""
    node = When(choice).then(double).otherwise(triple)
    templates = node.get_templates(
        step_counter=1,
        default_image="image",
        image_pull_policy="Always",
        default_secrets=None,
        default_parameters=[],
    )

    assert templates[0].name == "step-1-when"
    assert templates[1].name == "step-1-when-choice"
    assert templates[2].name == "step-1-when-then-double"
    assert templates[3].name == "step-1-when-otherwise-triple"


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
