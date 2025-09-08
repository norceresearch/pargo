from json import dumps

from argus.nodes.step import StepNode
from argus.utils import double


def test_stepnode_run(tmp_path):
    """Test that StepNode.run produce the expected output"""
    (tmp_path / ".argus" / "data.json").write_text(dumps({"x": 3}))

    node = StepNode(task=double)
    result = node.run(write_data=True)
    assert "x" in result
    assert result["x"] == 6


def test_stepnode_to_argo():
    """Test that StepNode.to_argo gives the expected format."""
    node = StepNode(task=double)
    steps, templates = node.to_argo(1)
    assert steps[0][0].name == "step1"
    assert templates[0].name == "step1"
    assert "python" in templates[0].script.command
