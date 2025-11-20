from __future__ import annotations

from json import dumps, loads
from os import environ
from typing import Any, Callable

from pydantic import Field

from ..argo_types.workflows import (
    Parameter,
    RetryStrategy,
    StepsTemplate,
    Task,
)
from .node import Node
from .run import argus_path, run_when
from .step import StepNode, StepTask
from .worker_template import worker_template

WhenTask = Callable[..., bool]


class When(Node):
    """
    Class for conditional execution of steps.
    """

    task: WhenTask = Field(description="Callable that returns a bool")
    image: str | None = Field(
        default=None, description="Overwrite workflow image for the WhenTask"
    )
    secrets: list[str] | None = Field(
        default=None, description="Overwrite workflow secrets for the WhenTask"
    )
    retry: int | RetryStrategy | None = Field(
        default=None, description="Overwrite workflow retry for the WhenTesk"
    )
    _then: StepNode | None = None
    _otherwise: StepNode | None = None
    _prev: str = "when"

    def __init__(self, task: WhenTask, **kwargs):
        super().__init__(task=task, **kwargs)

    @property
    def argo_name(self):
        """Name of the task."""
        return "when"

    def then(self, task: StepTask, **kwargs) -> When:
        """Set the task to exectue when the condition evaluates to True."""
        if self._prev != "when":
            raise RuntimeError(".then(...) must follow When(...) ")
        self._then = StepNode(task=task, **kwargs)
        self._prev = "then"
        return self

    def otherwise(self, task: StepTask, **kwargs) -> When:
        """Set the optional task to execute when the condition evaluates to False."""
        if self._prev != "then":
            raise RuntimeError(".otherwise(...) must follow then(...) ")
        self._otherwise = StepNode(task=task, **kwargs)
        self._prev = "otherwise"
        return self

    def run(self):
        """Run the When-block locally."""
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
        default_retry: int | RetryStrategy | None,
    ):
        """Returns a list with the configured templates (StepsTemplate and ScriptTemplates). @private"""
        block_name = f"step-{step_counter}-{self.argo_name}"
        when_name = block_name + "-" + self.task.__name__.lower().replace("_", "-")
        then_name = block_name + "-then-" + self._then.argo_name

        templates = [self._get_steps(block_name, default_parameters)]

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

    def _get_steps(self, block_name: str, default_parameters: dict[str, Any]):
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
        steps = StepsTemplate(
            name=block_name,
            inputs={"parameters": [Parameter(name="inputs", default=default)]},
            steps=[],
            outputs={
                "parameters": [
                    Parameter(
                        name="outputs",
                        valueFrom={"expression": expression},
                    ),
                ]
            },
        )

        parameters = [
            Parameter(
                name="inputs",
                value="{{inputs.parameters.inputs}}",
            )
        ]

        steps.steps.append(
            [
                Task(
                    name=when_name,
                    template=when_name,
                    arguments={"parameters": parameters},
                )
            ]
        )

        decision_steps = [
            Task(
                name=then_name,
                template=then_name,
                when=f"{{{{steps.{when_name}.outputs.parameters.outputs}}}} == true",
                arguments={"parameters": parameters},
            )
        ]
        if self._otherwise is not None:
            decision_steps.append(
                Task(
                    name=otherwise_name,
                    template=otherwise_name,
                    when=f"{{{{steps.{when_name}.outputs.parameters.outputs}}}} == false",
                    arguments={"parameters": parameters},
                )
            )

        steps.steps.append(decision_steps)
        return steps
