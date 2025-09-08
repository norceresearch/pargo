from json import loads
from os import environ
from pathlib import Path

from argus.nodes.init import InitNode


def test_initnode_data(tmp_path):
    """Test that InitNode insert parameters to data-file"""
    task_data = {"x": 42, "param2": "thanks for all the fish"}
    node = InitNode(task=task_data)
    node.run()

    data_path = Path(environ["ARGUS_DIR"]) / "data.json"
    result = loads(data_path.read_text())
    for k, v in task_data.items():
        assert result[k] == v


def test_initnode_to_argo():
    """Test that InitNode.to_argo give the expected format."""
    task_data = {"x": 1, "y": 2}
    node = InitNode(task=task_data)

    steps, templates = node.to_argo(step_counter=0)

    assert isinstance(steps, list)
    assert steps[0][0].name == "step0"

    assert isinstance(templates, list)
    template = templates[0]
    assert template.name == "init"
    env_names = [e.name for e in template.script.env]
    for k in task_data.keys():
        assert f"ARGUS_PARAM_{k}" in env_names
    assert any("ARGUS_DIR" in e.name for e in template.script.env)
