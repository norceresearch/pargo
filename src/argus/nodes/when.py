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
from ..run import merge_when, run_when
from .node import Node
from .step import StepNode, StepTask

WhenTask = Callable[..., bool]


class When(Node):
    task: WhenTask
    task_name: str = ""
    image: str | None = None
    secrets: list[str] | None = None
    _then: StepTask | None = None
    _otherwise: StepTask | None = None
    _prev: str = "when"

    def __init__(self, task: WhenTask, **kwargs):
        super().__init__(task=task, **kwargs)

    def model_post_init(self, __context):
        self.task_name = self.task.__name__

    def then(self, task: StepTask, **kwargs) -> When:
        if self._prev != "when":
            raise RuntimeError(".then(...) must follow When(...) ")
        self._then = StepNode(task=task, **kwargs)
        self._prev = "then"
        return self

    def otherwise(self, task: StepTask, **kwargs) -> When:
        if self._prev != "then":
            raise RuntimeError(".otherwise(...) must follow then(...) ")
        self._otherwise = StepNode(task=task, **kwargs)
        self._prev = "otherwise"
        return self

    def run(self):
        data = loads(Path("/tmp/data.json").read_text())
        environ["ARGUS_DATA"] = dumps(data)
        result = run_when(self.task_name, self.task.__module__)
        if result is True:
            self._then.run()
        if result is False:
            self._otherwise.run()

    def to_argo(self, step_counter: int):
        when_step, when_templates = self.to_argo_when(step_counter=step_counter)
        then_step, then_templates = self.to_argo_then(step_counter=step_counter)
        otherwise_step, otherwise_templates = self.to_argo_otherwise(
            step_counter=step_counter
        )
        merge_step, merge_templates = self.to_argo_merge(step_counter=step_counter)

        steps = [[when_step], [then_step, otherwise_step], [merge_step]]
        templates = (
            when_templates + then_templates + otherwise_templates + merge_templates
        )
        return steps, templates

    def to_argo_when(self, step_counter: int):
        script_source = f'from {run_when.__module__} import run_when\nrun_when("{self.task_name}", "{self.task.__module__}")'

        step_name = f"step{step_counter}when"
        parameters = [
            ArgoParameter(
                name="inputs",
                value=f"{{{{steps.step{step_counter - 1}.outputs.parameters.outputs}}}}",
            )
        ]
        secrets = None
        if self.secrets:
            secrets = [
                ArgoSecretRef(secretRef=ArgoParameter(name=secret))
                for secret in self.secrets
            ]
        templates = [
            ArgoScriptTemplate(
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
                        ArgoParameter(
                            name="outputs", valueFrom={"path": "/tmp/when.json"}
                        )
                    ]
                },
            )
        ]
        when_step = ArgoStep(
            name=step_name,
            template=self.task_name,
            arguments={"parameters": parameters},
        )
        return when_step, templates

    def to_argo_then(self, step_counter: int):
        step, templates = self._then.to_argo(step_counter, "then")
        step = step[0][0]  # unpack
        step.when = (
            f"{{{{steps.step{step_counter}when.outputs.parameters.outputs}}}} == true"
        )
        return step, templates

    def to_argo_otherwise(self, step_counter: int):
        step, templates = self._otherwise.to_argo(step_counter, "otherwise")
        step = step[0][0]  # unpack
        step.when = (
            f"{{{{steps.step{step_counter}when.outputs.parameters.outputs}}}} == false"
        )
        return step, templates

    def to_argo_merge(self, step_counter: int):
        parameters = [
            ArgoParameter(
                name="then_output",
                value=f"{{{{steps.step{step_counter}then.outputs.parameters.outputs}}}}",
            ),
            ArgoParameter(
                name="otherwise_output",
                value=f"{{{{steps.step{step_counter}otherwise.outputs.parameters.outputs}}}}",
            ),
        ]
        env = [
            ArgoParameter(name="ARGUS_THEN", value="{{inputs.parameters.then_output}}"),
            ArgoParameter(
                name="ARGUS_OTHER", value="{{inputs.parameters.otherwise_output}}"
            ),
        ]
        source = f"from {merge_when.__module__} import merge_when\nmerge_when()"
        templates = [
            (
                ArgoScriptTemplate(
                    name="whenmerge",
                    script=ArgoScript(
                        image=self.image, command=["python"], source=source, env=env
                    ),
                    inputs={
                        "parameters": [
                            ArgoParameter(name="then_output", default=""),
                            ArgoParameter(name="otherwise_output", default=""),
                        ]
                    },
                    outputs={
                        "parameters": [
                            ArgoParameter(
                                name="outputs", valueFrom={"path": "/tmp/data.json"}
                            )
                        ]
                    },
                )
            )
        ]

        merge_step = ArgoStep(
            name=f"step{step_counter}",
            template="whenmerge",
            arguments={"parameters": parameters},
        )
        return merge_step, templates
