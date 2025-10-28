from __future__ import annotations

from json import dumps, loads
from os import environ
from typing import Any, Callable

from ..argo_types.workflows import (
    ArgoParameter,
    ArgoRetryStrategy,
    ArgoStep,
    ArgoStepsTemplate,
)
from .node import Node
from .run import argus_path, run_when
from .step import StepNode, StepTask
from .worker_template import worker_template

WhenTask = Callable[..., bool]


class When(Node):
    task: WhenTask
    image: str | None = None
    secrets: list[str] | None = None
    retry: int | ArgoRetryStrategy | None = None
    _then: StepNode | None = None
    _otherwise: StepNode | None = None
    _prev: str = "when"

    def __init__(self, task: WhenTask, **kwargs):
        super().__init__(task=task, **kwargs)

    @property
    def argo_name(self):
        return "when"

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
        data_path = argus_path() / "data.json"
        data = loads(data_path.read_text())
        environ["ARGUS_DATA"] = dumps(data)
        result = run_when(self.task.__name__, self.task.__module__)
        if result is True:
            self._then.run()
        if result is False and self._otherwise is not None:
            self._otherwise.run()

    def get_templates(
        self,
        step_counter: int,
        default_image: str,
        image_pull_policy: str,
        default_secrets: list[str] | None,
        default_parameters: dict[str, Any],
        default_retry: int | ArgoRetryStrategy | None,
    ):
        block_name = f"step-{step_counter}-{self.argo_name}"
        when_name = block_name + "-" + self.task.__name__.lower().replace("_", "-")
        then_name = block_name + "-then-" + self._then.argo_name

        templates = [self.get_steps(block_name, default_parameters)]

        # when template
        script_source = f'from {run_when.__module__} import run_when\nrun_when("{self.task.__name__}", "{self.task.__module__}")'
        template = worker_template(
            template_name=when_name,
            script_source=script_source,
            parameters=default_parameters,
            image=self.image or default_image,
            image_pull_policy=image_pull_policy,
            secrets=self.secrets or default_secrets,
            parallelism=None,
            outpath="/tmp/when.json",
            retry=self.retry or default_retry,
        )
        templates.append(template)

        template = self._then.get_templates(
            step_counter=step_counter,
            default_image=default_image,
            image_pull_policy=image_pull_policy,
            default_secrets=default_secrets,
            default_parameters=default_parameters,
            default_retry=self.retry or default_retry,
        )
        template[0].name = then_name
        templates.extend(template)

        if self._otherwise is not None:
            otherwise_name = block_name + "-otherwise-" + self._otherwise.argo_name
            template = self._otherwise.get_templates(
                step_counter=step_counter,
                default_image=default_image,
                image_pull_policy=image_pull_policy,
                default_secrets=default_secrets,
                default_parameters=default_parameters,
                default_retry=self.retry or default_retry,
            )
            template[0].name = otherwise_name
            templates.extend(template)

        return templates

    def get_steps(self, block_name: str, default_parameters: dict[str, Any]):
        when_name = block_name + "-" + self.task.__name__.lower().replace("_", "-")
        then_name = block_name + "-then-" + self._then.argo_name
        default = ",".join(
            f'"{k}": {{{{workflow.parameters.{k}}}}}' for k in default_parameters
        )
        default = f"{{{default}}}"

        if self._otherwise is None:
            expression = f'steps["{when_name}"].outputs.parameters.outputs == "true" ? steps["{then_name}"].outputs.parameters.outputs : inputs.parameters.inputs'
        else:
            otherwise_name = block_name + "-otherwise-" + self._otherwise.argo_name
            expression = f'steps["{when_name}"].outputs.parameters.outputs == "true" ? steps["{then_name}"].outputs.parameters.outputs : steps["{otherwise_name}"].outputs.parameters.outputs'
        steps = ArgoStepsTemplate(
            name=block_name,
            inputs={"parameters": [ArgoParameter(name="inputs", default=default)]},
            steps=[],
            outputs={
                "parameters": [
                    ArgoParameter(
                        name="outputs",
                        valueFrom={"expression": expression},
                    ),
                ]
            },
        )

        parameters = [
            ArgoParameter(
                name="inputs",
                value="{{inputs.parameters.inputs}}",
            )
        ]

        steps.steps.append(
            [
                ArgoStep(
                    name=when_name,
                    template=when_name,
                    arguments={"parameters": parameters},
                )
            ]
        )

        decision_steps = [
            ArgoStep(
                name=then_name,
                template=then_name,
                when=f"{{{{steps.{when_name}.outputs.parameters.outputs}}}} == true",
                arguments={"parameters": parameters},
            )
        ]
        if self._otherwise is not None:
            decision_steps.append(
                ArgoStep(
                    name=otherwise_name,
                    template=otherwise_name,
                    when=f"{{{{steps.{when_name}.outputs.parameters.outputs}}}} == false",
                    arguments={"parameters": parameters},
                )
            )

        steps.steps.append(decision_steps)
        return steps
