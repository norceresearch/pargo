from __future__ import annotations

from json import dumps, loads
from os import environ
from typing import Callable

from ..argo_types.workflows import (
    ArgoParameter,
    ArgoScript,
    ArgoScriptTemplate,
    ArgoSecretRef,
    ArgoStep,
)
from .node import Node
from .run import argus_path, run_step

StepTask = Callable[..., None | dict]


class StepNode(Node):
    task: StepTask
    image: str | None = None
    secrets: list[str] | None = None
    parallelism: int | None = None

    def run(self, write_data: bool = True):
        data_path = argus_path() / "data.json"
        data = loads(data_path.read_text())
        environ["ARGUS_DATA"] = dumps(data)
        result = run_step(
            self.task.__name__, self.task.__module__, write_data=write_data
        )
        return result

    def to_argo(self, step_counter: int, step_suffix: str = ""):
        script_source = f'from {run_step.__module__} import run_step\nrun_step("{self.task.__name__}", "{self.task.__module__}")'

        step_name = f"step{step_counter}{step_suffix}"
        parameters = [
            ArgoParameter(
                name="inputs",
                value=f"{{{{steps.step{step_counter - 1}.outputs.parameters.outputs}}}}",
            )
        ]
        step = ArgoStep(
            name=step_name,
            template=step_name,
            arguments={"parameters": parameters},
        )
        secrets = None
        if self.secrets:
            secrets = [
                ArgoSecretRef(secretRef=ArgoParameter(name=secret))
                for secret in self.secrets
            ]
        image_pull_policy = "Always" if self.image else None
        template = ArgoScriptTemplate(
            name=step_name,
            script=ArgoScript(
                image=self.image,
                command=["python"],
                source=script_source,
                env=[
                    ArgoParameter(
                        name="ARGUS_DATA", value="{{inputs.parameters.inputs}}"
                    ),
                    ArgoParameter(name="ARGUS_DIR", value="/tmp"),
                ],
                envFrom=secrets,
                imagePullPolicy=image_pull_policy,
            ),
            inputs={"parameters": [ArgoParameter(name="inputs")]},
            outputs={
                "parameters": [
                    ArgoParameter(name="outputs", valueFrom={"path": "/tmp/data.json"})
                ]
            },
            parallelism=self.parallelism,
        )

        return [[step]], [template]
