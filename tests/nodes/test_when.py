import pytest

from pargo import When
from pargo.utils import choice, double, false, triple, true


def test_when_branch_then(tmp_path):
    """Test that then branch runs when expected."""
    data = {"x": 3}

    node = When(choice).then(double).otherwise(triple)
    result = node.run(data)

    assert result["x"] == 6


def test_when_branch_otherwise(tmp_path):
    """Test that otherwise branch runs when expected."""
    data = {"x": 4}

    node = When(choice).then(double).otherwise(triple)
    result = node.run(data)

    assert result["x"] == 12


def test_when_branch_then_only(tmp_path):
    """Test that then branch runs when expected."""
    data = {"x": 3}
    node = When(choice).then(double)
    result = node.run(data)
    assert result["x"] == 6

    data = {"x": 4}
    node = When(choice).then(double)
    result = node.run(data)
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
        default_retry=None,
    )

    assert templates[0].name == "step-1-when-0"
    assert templates[1].name == "step-1-choice"
    assert templates[2].name == "step-1-double"
    assert templates[3].name == "step-1-triple"


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


def test_when_nested_run():
    data = {"x": 3}

    node = When(choice).then(When(true).then(double).otherwise(triple))
    result = node.run(data)

    assert "x" in result
    assert isinstance(result["x"], int)
    assert result["x"] == 6

    node = When(choice).then(When(false).then(double).otherwise(triple))
    result = node.run(data)

    assert "x" in result
    assert isinstance(result["x"], int)
    assert result["x"] == 9
