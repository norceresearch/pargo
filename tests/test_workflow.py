import subprocess
import sys
from json import dumps, loads
from os import environ
from pathlib import Path
from shutil import which

import pytest
from pydantic_core._pydantic_core import ValidationError

from argus import Foreach, When, Workflow
from argus.utils import add_item, choice, double, get_items, triple


def lint_yaml(tmp_path):
    """Offline lint of manifests. Install argo cli from https://github.com/argoproj/argo-workflows/releases"""
    result = subprocess.run(
        ["argo", "lint", str(tmp_path / "*.yaml"), "--offline"],
        shell=True,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_workflow_run(tmp_path):
    """Test that Workflow.run runs without error and produce expected output."""
    testflow = Workflow.new("testflow", parameters={"x": 2}).next(double).next(triple)
    testflow.run()

    data_path = tmp_path / ".argus" / "data.json"
    data = loads(data_path.read_text())
    assert data["x"] == 12


def test_workflow_missing_parameter():
    """Test that Workflow.run fails for missing parameter."""
    testflow = Workflow.new("testflow").next(double).next(triple)
    with pytest.raises(TypeError):
        testflow.run()


def test_workflow_yaml(tmp_path):
    """Test that Workflow.to_yaml produce a yaml file"""
    testflow = Workflow.new("testflow", parameters={"x": 2}).next(double).next(triple)

    yaml_path = tmp_path / "testflow.yaml"
    testflow.to_yaml(path=tmp_path)
    assert yaml_path.exists()
    data = yaml_path.read_text()
    assert "WorkflowTemplate" in data
    assert "testflow" in data

    if which("argo"):
        lint_yaml(tmp_path)


def test_workflow_yaml_consistency(tmp_path):
    """Test that Workflow.to_yaml always produce a consistent yaml."""
    yamls = []
    for _ in range(2):
        (
            Workflow.new("testflow", parameters={"x": 2})
            .next(double)
            .to_yaml(path=tmp_path)
        )
        yamls.append((tmp_path / "testflow.yaml").read_text())
    assert yamls[0] == yamls[1], "Indentical workflows give inconsistent yamls."


def test_workflow_to_argo():
    """Test that to_argo produce the expected structure"""
    testflow = Workflow.new(
        name="testflow",
        parameters={
            "x": 1,
        },
        image="image",
        secrets=["minio-s3-credentials-secret", "mlflow-credentials-secret"],
        schedules=["0 0 * * *"],
        parallelism=3,
    ).next(double, image="other-image", parallelism=5)
    argo_testflow = testflow.to_argo()
    assert argo_testflow.kind == "WorkflowTemplate"
    assert argo_testflow.metadata.name == "testflow"
    assert argo_testflow.spec.entrypoint == "main"
    assert argo_testflow.spec.parallelism == 3
    assert argo_testflow.spec.arguments["parameters"][0].name == "x"
    assert argo_testflow.spec.templates[0].name == "main"
    assert len(argo_testflow.spec.templates[0].steps) == 2

    # init step
    step0 = argo_testflow.spec.templates[1]
    assert step0.name == "step0"
    assert step0.script.image == "image"
    assert step0.script.env[0].name == "ARGUS_PARAM_x"

    # double step
    step1 = argo_testflow.spec.templates[2]
    assert step1.name == "step1"
    assert step1.script.image == "other-image"
    assert step1.script.env[0].name == "ARGUS_DATA"
    assert step1.script.envFrom[0].secretRef.name == "minio-s3-credentials-secret"
    assert step1.parallelism == 5


def test_workflow_duplicate_templates():
    """Test that duplicated templates are allowed."""

    testflow = Workflow.new("testflow", parameters={"x": 1})
    argo_testflow = (
        testflow.next(double).next(double).next(double, image="OTHER_IMAGE").to_argo()
    )
    assert argo_testflow.spec.templates[2].name == "step1"
    assert argo_testflow.spec.templates[3].name == "step2"
    assert argo_testflow.spec.templates[4].name == "step3"

    testflow.run()
    data_path = Path(environ["ARGUS_DIR"]) / "data.json"
    result = loads(data_path.read_text())
    assert result["x"] == 8


def test_workflow_schedule(tmp_path):
    """Test that Workflow.to_yaml produces an additional cron-yaml"""
    testflow = Workflow.new("testflow", schedules=["0 0 0 * *"]).next(double)

    yaml_path = tmp_path / "testflow-cron.yaml"
    testflow.to_yaml(path=tmp_path)
    assert yaml_path.exists()
    data = yaml_path.read_text()
    assert "CronWorkflow" in data
    assert "testflow" in data
    if which("argo"):
        lint_yaml(tmp_path)


def test_workflow_trigger_on(tmp_path):
    """Test that Workflow.to_yaml produces an additional sensor-yaml"""
    testflow = Workflow.new("testflow").next(double)
    triggeredflow = Workflow.new("triggeredflow", trigger_on=testflow).next(double)

    yaml_path = tmp_path / "triggeredflow-sensor.yaml"
    triggeredflow.to_yaml(path=tmp_path)
    assert yaml_path.exists()
    data = yaml_path.read_text()
    assert "Sensor" in data
    assert "testflow" in data
    assert "triggeredflow" in data
    if which("argo"):
        lint_yaml(tmp_path)


def test_workflow_trigger_on_multiple(tmp_path):
    """Test that Workflow.to_yaml produces an additional sensor-yaml"""
    testflow1 = Workflow.new("testflow1").next(double)
    testflow2 = Workflow.new("testflow2").next(double)
    triggeredflow = Workflow.new(
        "triggeredflow", trigger_on=testflow1 | testflow1 & testflow2
    ).next(double)

    yaml_path = tmp_path / "triggeredflow-sensor.yaml"
    triggeredflow.to_yaml(path=tmp_path)
    assert yaml_path.exists()
    data = yaml_path.read_text()
    assert "Sensor" in data
    assert "triggeredflow" in data
    if which("argo"):
        lint_yaml(tmp_path)


def test_workflow_trigger_on_multiple_error(tmp_path):
    """Test that Workflow.to_yaml fails for incorrect triggering syntax."""
    testflow1 = Workflow.new("testflow1").next(double)
    testflow2 = Workflow.new("testflow2").next(double)
    with pytest.raises(ValueError):
        Workflow.new(
            "triggeredflow", trigger_on=testflow1 & (testflow1 | testflow2)
        ).next(double)

    with pytest.raises(ValueError):
        Workflow.new(
            "triggeredflow", trigger_on=(testflow1 | testflow2) & testflow2
        ).next(double)


def test_workflow_trigger_on_params(tmp_path):
    """Test that Workflow.to_yaml produces an additional sensor-yaml with parameters."""
    testflow1 = Workflow.new("testflow1").next(double)
    testflow2 = Workflow.new("testflow2").next(double)
    triggeredflow = Workflow.new(
        "triggeredflow",
        trigger_on=testflow1 | testflow2,
        trigger_on_parameters=[{"x": 1}, {"x": 2}],
    ).next(double)

    yaml_path = tmp_path / "triggeredflow-sensor.yaml"
    triggeredflow.to_yaml(path=tmp_path)
    assert yaml_path.exists()
    data = yaml_path.read_text()
    assert "Sensor" in data
    assert "parameters" in data
    if which("argo"):
        lint_yaml(tmp_path)


def test_workflow_trigger_on_params_error(tmp_path):
    """Test that Workflow.to_yaml fails for incorrect number of parameters."""
    testflow1 = Workflow.new("testflow1").next(double)
    testflow2 = Workflow.new("testflow2").next(double)
    with pytest.raises(ValidationError):
        Workflow.new(
            "triggeredflow",
            trigger_on=testflow1 | testflow2,
            trigger_on_parameters=[{"x": 1}],
        )


def test_workflow_complex(tmp_path):
    """Test run and to_yaml for a complex workflow."""
    testflow = (
        Workflow.new(
            name="testflow",
            parameters={
                "x": 1,
                "param2": "value2",
                "param3": 3.0,
                "param4": dumps({"c": 1, "d": 2}),
            },
            image="image",
            secrets=["minio-s3-credentials-secret", "mlflow-credentials-secret"],
            schedules=["0 0 * * *"],
        )
        .next(double)
        .next(When(choice).then(double).otherwise(triple))
        .next(Foreach(get_items).then(double))
        .next(Foreach([1, 5, 3], item_name="item").then(add_item))
    )

    testflow.run()
    data_path = Path(environ["ARGUS_DIR"]) / "data.json"
    result = loads(data_path.read_text())
    assert sorted(result["x"]) == [13, 15, 17]

    testflow.to_yaml(path=tmp_path)
    assert (tmp_path / "testflow.yaml").exists()
    assert (tmp_path / "testflow-cron.yaml").exists()
    if which("argo"):
        lint_yaml(tmp_path)


def test_workfow_main(capsys):
    """Test that main prints how to run workflow and generate manifest."""
    testflow = Workflow.new(name="testflow")
    testflow.main()
    out = capsys.readouterr().out
    assert "Argus workflow testflow" in out


def test_workflow_main_run(monkeypatch):
    """Test that the workflow runs with command run."""
    testflow = Workflow.new(name="testflow", parameters={"x": 1}).next(double)

    monkeypatch.setattr(sys, "argv", ["mock.py", "run"])

    testflow.main()
    data_path = Path(environ["ARGUS_DIR"]) / "data.json"
    result = loads(data_path.read_text())
    assert result["x"] == 2


def test_workflow_main_run_params(monkeypatch):
    """Test that the workflow runs with default and override parameters."""
    defaults = {
        "int_param": 2,
        "bool_param": False,
        "float_param": 1.5,
        "str_param": "parameter",
        "json_param": dumps({"x": 1}),
    }
    testflow = Workflow.new(name="testflow", parameters=defaults)

    monkeypatch.setattr(sys, "argv", ["wf.py", "run"])
    testflow.main()
    data_path = Path(environ["ARGUS_DIR"]) / "data.json"
    result = loads(data_path.read_text())
    assert result["int_param"] == 2
    assert result["bool_param"] is False
    assert result["float_param"] == 1.5
    assert result["str_param"] == "parameter"
    assert result["json_param"] == dumps({"x": 1})

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "wf.py",
            "run",
            "--params",
            "int_param",
            "5",
            "bool_param",
            "true",
            "float_param",
            "2.5",
            "str_param",
            "other",
            "json_param",
            dumps(dumps({"x": 5})),
        ],
    )
    testflow.main()
    data_path = Path(environ["ARGUS_DIR"]) / "data.json"
    result = loads(data_path.read_text())
    assert result["int_param"] == 5
    assert result["bool_param"] is True
    assert result["float_param"] == 2.5
    assert result["str_param"] == "other"
    assert result["json_param"] == dumps({"x": 5})


def test_workflow_main_run_missing_value(monkeypatch):
    """Test that the run fails if only key without value is provided"""
    testflow = Workflow.new(name="testflow")
    monkeypatch.setattr(sys, "argv", ["mock.py", "run", "--params", "foo"])
    with pytest.raises(ValueError, match="Missing value"):
        testflow.main()


def test_workflow_main_generate(monkeypatch, tmp_path):
    """Test that the manifest is generated with command generate."""
    testflow = Workflow.new(name="testflow", parameters={"x": 1}).next(double)
    monkeypatch.setattr(sys, "argv", ["mock.py", "generate"])

    testflow.main(tmp_path)

    out_file = tmp_path / "testflow.yaml"
    assert out_file.exists()
    content = out_file.read_text()
    assert "kind: WorkflowTemplate" in content
    assert "metadata" in content
