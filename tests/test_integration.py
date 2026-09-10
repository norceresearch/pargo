"""End-to-end tests: apply the generated manifests to a real Argo Workflows and run them.

Skipped unless PARGO_INTEGRATION=1. The `integration` job in .github/workflows/CI.yml
sets up the cluster (kind + Argo); to run locally, point kubectl at a throwaway cluster
with Argo installed in PARGO_NAMESPACE and do the same.
"""

import json
import subprocess
from os import environ

import pytest

from pargo.config import NAMESPACE
from pargo.examples import EXAMPLES

TIMEOUT = int(environ.get("PARGO_TIMEOUT", 600))

pytestmark = pytest.mark.skipif(
    environ.get("PARGO_INTEGRATION") != "1",
    reason="needs a cluster running Argo Workflows; set PARGO_INTEGRATION=1",
)


def sh(*args: str, check: bool = True) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=TIMEOUT)
    if check and result.returncode != 0:
        raise AssertionError(
            f"{' '.join(args)} failed:\n{result.stdout}\n{result.stderr}"
        )
    return result.stdout


@pytest.fixture(scope="module")
def templates(tmp_path_factory):
    """Apply every example as a WorkflowTemplate.

    Every xdist worker runs this. A client-side apply would race (each worker
    reads "not found", then all try to create); server-side apply is a single
    upsert, so concurrent identical applies are fine.
    """
    path = tmp_path_factory.mktemp("manifests")
    for workflow in EXAMPLES:
        workflow.to_yaml(path)
    sh(
        "kubectl",
        "apply",
        "--server-side",
        "--force-conflicts",
        "-n",
        NAMESPACE,
        "-f",
        str(path),
    )


def submit(workflow) -> str:
    """Submit a workflow from its template, returning the generated name."""
    out = sh(
        "argo",
        "submit",
        "--from",
        f"workflowtemplate/{workflow.name}",
        "-n",
        NAMESPACE,
        "--output",
        "json",
    )
    return json.loads(out)["metadata"]["name"]


@pytest.mark.parametrize(
    "workflow,expected",
    list(EXAMPLES.items()),
    ids=lambda arg: getattr(arg, "name", str(arg)),
)
def test_example_runs(templates, workflow, expected):
    name = submit(workflow)

    # argo wait exits non-zero for a failed workflow, which is a valid outcome here,
    # so the phase below is what the test actually asserts on.
    sh("argo", "wait", name, "-n", NAMESPACE, check=False)

    phase = sh(
        "kubectl",
        "get",
        "workflow",
        name,
        "-n",
        NAMESPACE,
        "-o",
        "jsonpath={.status.phase}",
    )
    assert phase == expected, sh("argo", "get", name, "-n", NAMESPACE, check=False)
