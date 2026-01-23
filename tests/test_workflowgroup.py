import subprocess
from json import loads
from shutil import which

from pargo import Workflow, WorkflowGroup
from pargo.utils import void


def lint_yaml(tmp_path):
    """Offline lint of manifests. Install argo cli from https://github.com/argoproj/argo-workflows/releases"""
    result = subprocess.run(
        ["argo", "lint", str(tmp_path / "*.yaml"), "--offline"],
        shell=True,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_workflowgroup_run(tmp_path):
    """Test that WorkflowGroup.run runs without error and produce expected output."""
    testflow1 = Workflow.new("testflow1", parameters={"x": 1}).next(void)
    testflow2 = Workflow.new("testflow2", parameters={"x": 2}).next(void)

    groupflow = (
        WorkflowGroup.new("groupflow")
        .next(testflow1)
        .next([testflow1, testflow2])
        .next([testflow2])
    )
    groupflow.run()

    data_path = tmp_path / ".pargo" / "data.json"
    data = loads(data_path.read_text())
    assert data["x"] == 2


def test_workflowgroup_yaml(tmp_path):
    """Test that WorkflowGroup.to_yaml produce a yaml file"""
    testflow1 = Workflow.new("testflow1", parameters={"x": 1}).next(void)
    testflow2 = Workflow.new("testflow2", parameters={"x": 2}).next(void)
    groupflow = WorkflowGroup.new("groupflow").next([testflow1, testflow2])

    yaml_path = tmp_path / "groupflow.yaml"
    groupflow.to_yaml(path=tmp_path)
    assert yaml_path.exists()
    data = yaml_path.read_text()
    assert "WorkflowTemplate" in data
    assert "groupflow" in data

    if which("argo"):
        lint_yaml(tmp_path)
