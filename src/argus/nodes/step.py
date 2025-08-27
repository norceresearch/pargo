from __future__ import annotations

from json import dumps, loads
from os import environ
from pathlib import Path
from typing import Callable

from ..argo_types.workflows import (
    ArgoParameter,
    ArgoScript,
    ArgoScriptTemplate,
    ArgoSecretRef,
    ArgoStep,
)
from ..run import run_step
from .node import Node

StepTask = Callable[..., None | dict]


class StepNode(Node):
    task: StepTask
    task_name: str = ""
    image: str | None = None
    secrets: list[str] | None = None

    def model_post_init(self, __context):
        self.task_name = self.task.__name__

    def run(self, write_data: bool = True):
        data = loads(Path("/tmp/data.json").read_text())
        environ["ARGUS_DATA"] = dumps(data)
        result = run_step(self.task_name, self.task.__module__, write_data=write_data)
        return result

    def to_argo(self, step_counter: int, step_suffix: str = ""):
        script_source = f'from {run_step.__module__} import run_step\nrun_step("{self.task_name}", "{self.task.__module__}")'

        step_name = f"step{step_counter}{step_suffix}"
        parameters = [
            ArgoParameter(
                name="inputs",
                value=f"{{{{steps.step{step_counter - 1}.outputs.parameters.outputs}}}}",
            )
        ]
        step = ArgoStep(
            name=step_name,
            template=self.task_name,
            arguments={"parameters": parameters},
        )
        secrets = None
        if self.secrets:
            secrets = [
                ArgoSecretRef(secretRef=ArgoParameter(name=secret))
                for secret in self.secrets
            ]

        template = ArgoScriptTemplate(
            name=self.task_name,
            script=ArgoScript(
                image=self.image,
                command=["python"],
                source=script_source,
                env=[
                    ArgoParameter(
                        name="ARGUS_DATA", value="{{inputs.parameters.inputs}}"
                    )
                ],
                envFrom=secrets,
            ),
            inputs={"parameters": [ArgoParameter(name="inputs")]},
            outputs={
                "parameters": [
                    ArgoParameter(name="outputs", valueFrom={"path": "/tmp/data.json"})
                ]
            },
        )

        return [[step]], [template]
