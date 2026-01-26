from pargo.nodes.step import StepNode
from pargo.utils import double


def test_stepnode_run(tmp_path):
    """Test that StepNode.run produce the expected output"""
    data = {"x": 3}

    node = StepNode(task=double)
    result = node.run(data)
    assert "x" in result
    assert result["x"] == 6


def test_stepnode_get_templates():
    """Test that StepNode.get_templates gives the expected format."""
    node = StepNode(task=double)
    templates = node.get_templates(
        step_counter=2,
        default_image="image",
        image_pull_policy="Always",
        default_secrets=None,
        default_parameters=[],
        default_retry=None,
    )

    assert templates[0].name == "step-2-double"
    assert "python" in templates[0].script.command
