import sys
from json import dumps, loads
from os import environ
from pathlib import Path

from pytest import raises

from argus.cli.main import cli


def write_workflow_file(tmp_path):
    """Create wf.py with a Workflow"""

    defaults = {
        "x": 1,
        "int_param": 5,
        "bool_param": False,
        "float_param": 1.5,
        "str_param": "parameter",
        "json_param": dumps({"y": 1}),
    }
    wf_path = tmp_path / "wf.py"
    wf_path.write_text(
        "from argus import Workflow\n"
        "from argus.utils import double\n"
        f"wf = Workflow.new(name='testflow', parameters={defaults}).next(double)\n"
    )
    return wf_path


def test_cli_run(monkeypatch, tmp_path):
    """Test that argus run executes the workflow."""

    wf_path = write_workflow_file(tmp_path)

    monkeypatch.setattr(sys, "argv", ["argus", "run", str(wf_path)])
    cli()

    data_path = Path(environ["ARGUS_DIR"]) / "data.json"
    data = loads(data_path.read_text())
    assert data["x"] == 2


def test_cli_run_with_defaults(monkeypatch, tmp_path):
    """Test that defaults are set correctly."""

    wf_path = write_workflow_file(tmp_path)
    monkeypatch.setattr(sys, "argv", ["argus", "run", str(wf_path)])
    cli()

    data_path = Path(environ["ARGUS_DIR"]) / "data.json"
    data = loads(data_path.read_text())
    assert data["x"] == 2
    assert data["int_param"] == 5
    assert data["bool_param"] is False
    assert data["float_param"] == 1.5
    assert data["str_param"] == "parameter"
    assert data["json_param"] == dumps({"y": 1})


def test_cli_run_with_params(monkeypatch, tmp_path):
    """Test that override parameters are set correctly."""

    wf_path = write_workflow_file(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "argus",
            "run",
            str(wf_path),
            "--param",
            "x=5",
            "--param",
            "int_param=10",
            "--param",
            "bool_param=true",
            "--param",
            "float_param=3.14",
            "--param",
            "str_param=other",
            "--param",
            f"json_param={dumps(dumps({'x': 2}))}",
        ],
    )
    cli()

    data_path = Path(environ["ARGUS_DIR"]) / "data.json"
    data = loads(data_path.read_text())
    assert data["x"] == 10
    assert data["int_param"] == 10
    assert data["bool_param"] is True
    assert data["float_param"] == 3.14
    assert data["str_param"] == "other"
    assert data["json_param"] == dumps({"x": 2})


def test_cli_run_param_missing_equals(monkeypatch, tmp_path):
    """Test that bad key=value fails"""
    wf_path = write_workflow_file(tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["argus", "run", str(wf_path), "--param", "badparam"]
    )
    with raises(ValueError, match="Expected key=value"):
        cli()


def test_cli_run_with_name(monkeypatch, tmp_path):
    """Test that selected workflow runs using --name"""
    wf_path = tmp_path / "wf.py"
    wf_path.write_text(
        "from argus import Workflow\n"
        "from argus.utils import double\n"
        "wf1 = Workflow.new(name='first', parameters={'x': 1}).next(double)\n"
        "wf2 = Workflow.new(name='second', parameters={'x': 2}).next(double)\n"
    )

    monkeypatch.setattr(sys, "argv", ["argus", "run", str(wf_path)])
    cli()
    data_path = Path(environ["ARGUS_DIR"]) / "data.json"
    data = loads(data_path.read_text())
    assert data["x"] == 4

    monkeypatch.setattr(sys, "argv", ["argus", "run", str(wf_path), "--name", "first"])
    cli()
    data = loads(data_path.read_text())
    assert data["x"] == 2


def test_cli_run_with_invalid_name(monkeypatch, tmp_path):
    """Test that unknown name fails."""
    wf_path = write_workflow_file(tmp_path)
    monkeypatch.setattr(sys, "argv", ["argus", "run", str(wf_path), "--name", "nope"])
    with raises(RuntimeError, match="No workflow named 'nope'"):
        cli()


def test_cli_generate(monkeypatch, tmp_path):
    """Test that argus generate produces manifest."""
    wf_path = write_workflow_file(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["argus", "generate", str(wf_path)])
    cli()

    out_file = tmp_path / "testflow.yaml"
    assert out_file.exists()
    content = out_file.read_text()
    assert "kind: WorkflowTemplate" in content
    assert "metadata" in content


def test_cli_generate_with_outdir(monkeypatch, tmp_path):
    """Test that manifest is written to optional location when provided."""

    wf_path = write_workflow_file(tmp_path)
    outdir = tmp_path / "out"
    outdir.mkdir()

    monkeypatch.setattr(
        sys, "argv", ["argus", "generate", str(wf_path), "--outdir", str(outdir)]
    )
    cli()

    out_file = outdir / "testflow.yaml"
    assert out_file.exists()


def test_cli_generate_with_name(monkeypatch, tmp_path):
    """Test that selected workflow manifest is generated using --name."""
    wf_path = tmp_path / "wf.py"
    wf_path.write_text(
        "from argus import Workflow\n"
        "wf1 = Workflow.new(name='first')\n"
        "wf2 = Workflow.new(name='second')\n"
    )

    outdir = tmp_path / "out"
    outdir.mkdir()

    monkeypatch.setattr(
        sys,
        "argv",
        ["argus", "generate", str(wf_path), "--name", "first", "--outdir", str(outdir)],
    )
    cli()

    out_file = outdir / "first.yaml"
    assert out_file.exists()
