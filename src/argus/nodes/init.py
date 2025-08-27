from __future__ import annotations

from json import dumps
from os import environ
from typing import Any

from ..argo_types.workflows import (
    ArgoParameter,
    ArgoScript,
    ArgoScriptTemplate,
    ArgoStep,
)
from ..run import run_init
from .node import Node

InitTask = dict[str, Any]


class InitNode(Node):
    task: InitTask
    task_name: str = "init"

    def run(self):
        for key, value in self.task.items():
            environ[f"ARGUS_PARAM_{key}"] = dumps(value)
        run_init()

    def to_argo(self, step_counter: int, step_suffix: str = ""):
        script_source = f"from {run_init.__module__} import run_init\nrun_init()"

        step_name = f"step{step_counter}{step_suffix}"
        step = ArgoStep(
            name=step_name,
            template=self.task_name,
        )

        env = [
            ArgoParameter(
                name=f"ARGUS_PARAM_{k}", value=f"{{{{workflow.parameters.{k}}}}}"
            )
            for k in self.task.keys()
        ]
        template = ArgoScriptTemplate(
            name=self.task_name,
            script=ArgoScript(
                image=None,
                command=["python"],
                source=script_source,
                env=env,
                imagePullPolicy="Always",
            ),
            outputs={
                "parameters": [
                    ArgoParameter(name="outputs", valueFrom={"path": "/tmp/data.json"})
                ]
            },
        )

        return [[step]], [template]
