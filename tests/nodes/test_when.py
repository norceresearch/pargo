import pytest

from pargo import When
from pargo.utils import choice, double, triple


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


def test_when_output_expression_tolerates_skipped_branches():
    """Skipped `then`/`otherwise` steps must fall back, not resolve to nil.

    Argo Workflows 4.0.7 / 3.7.16 made an output expression that evaluates to
    nil a terminal error unless a fallback is declared. Exactly one of the two
    branches is always skipped, so both dereferences need one.
    """
    node = When(choice).then(double).otherwise(triple)
    templates = node.get_templates(
        step_counter=1,
        default_image="image",
        image_pull_policy="Always",
        default_secrets=None,
        default_parameters=[],
        default_retry=None,
    )

    value_from = templates[0].outputs["parameters"][0].valueFrom
    expression = value_from["expression"]

    assert expression.count("?? inputs.parameters.inputs") == 2
    assert "default" in value_from


def test_when_output_expression_without_otherwise():
    """Without `otherwise` the false branch passes inputs straight through."""
    node = When(choice).then(double)
    templates = node.get_templates(
        step_counter=1,
        default_image="image",
        image_pull_policy="Always",
        default_secrets=None,
        default_parameters=[],
        default_retry=None,
    )

    value_from = templates[0].outputs["parameters"][0].valueFrom

    assert value_from["expression"].endswith(": (inputs.parameters.inputs)")
    assert "default" in value_from
