from __future__ import annotations

from typing import Any, Callable

from pydantic import Field

from ..argo_types.workflows import (
    Parameter,
    RetryStrategy,
    StepsTemplate,
    Task,
)
from .import_path import import_path
from .node import Node
from .run import run_when
from .step import StepNode
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
    _then: Node | None = None
    _otherwise: Node | None = None
    _prev: str = "when"

    def __init__(self, task: WhenTask, **kwargs):
        super().__init__(task=task, **kwargs)

    @property
    def task_name(self):
        """Name of the when-task."""
        return self.task.__name__

    @property
    def argo_name(self):
        """Argo friendly name of the when-task."""
        return self.task_name.lower().replace("_", "-")

    @property
    def task_module(self):
        """Module of the task."""
        if self.task.__module__ and self.task.__module__ != "__main__":
            return self.task.__module__
        else:
            return import_path(self.task)

    def then(self, node: Callable | Node, **kwargs) -> When:
        """Set the task to exectue when the condition evaluates to True."""
        if self._prev != "when":
            raise RuntimeError(".then(...) must follow When(...) ")
        if callable(node):
            node = StepNode(task=node, **kwargs)
        self._then = node
        self._prev = "then"
        return self

    def otherwise(self, node: Callable | Node, **kwargs) -> When:
        """Set the optional task to execute when the condition evaluates to False."""
        if self._prev != "then":
            raise RuntimeError(".otherwise(...) must follow then(...) ")
        if callable(node):
            node = StepNode(task=node, **kwargs)
        self._otherwise = node
        self._prev = "otherwise"
        return self

    def run(self, data: dict[str, Any]):
        """Run the When-block locally."""
        result = run_when(self.task_name, self.task_module, data)
        if result is True:
            data = self._then.run(data)
        if result is False and self._otherwise is not None:
            data = self._otherwise.run(data)
        return data

    def get_templates(
        self,
        step_counter: int,
        default_image: str,
        image_pull_policy: str,
        default_secrets: list[str] | None,
        default_parameters: dict[str, Any],
        default_retry: int | RetryStrategy | None,
        when_level: int = 0,
        foreach_level: int = 0,
    ):
        """Returns a list with the configured templates (StepsTemplate and ScriptTemplates). @private"""

        script_source = f'from {run_when.__module__} import run_when\nrun_when("{self.task_name}", "{self.task_module}")'
        when_template = worker_template(
            template_name=f"step-{step_counter}-{self.argo_name}",
            script_source=script_source,
            parameters=default_parameters,
            image=self.image or default_image,
            image_pull_policy=image_pull_policy,
            secrets=self.secrets or default_secrets,
            parallelism=None,
            outpath="/tmp/when.json",
            retry=self.retry or default_retry,
        )

        then_templates = self._then.get_templates(
            step_counter=step_counter,
            default_image=default_image,
            image_pull_policy=image_pull_policy,
            default_secrets=default_secrets,
            default_parameters=default_parameters,
            default_retry=self.retry or default_retry,
            when_level=when_level + 1,
            foreach_level=foreach_level,
        )

        if self._otherwise is not None:
            otherwise_templates = self._otherwise.get_templates(
                step_counter=step_counter,
                default_image=default_image,
                image_pull_policy=image_pull_policy,
                default_secrets=default_secrets,
                default_parameters=default_parameters,
                default_retry=self.retry or default_retry,
                when_level=when_level + 1,
                foreach_level=foreach_level,
            )
            otherwise_name = otherwise_templates[0].name
        else:
            otherwise_name = None

        block_name = f"step-{step_counter}-when-{when_level}"
        block_template = self._get_steps(
            block_name,
            when_template.name,
            then_templates[0].name,
            otherwise_name,
            default_parameters,
        )
        templates = [block_template, when_template, *then_templates]
        if self._otherwise is not None:
            templates.extend(otherwise_templates)
        return templates

    def _get_steps(
        self,
        block_name: str,
        when_name: str,
        then_name: str,
        otherwise_name: str,
        default_parameters: dict[str, Any],
    ):
        default = ",".join(
            f'"{k}": {{{{workflow.parameters.{k}}}}}' for k in default_parameters
        )
        default = f"{{{default}}}"

        if self._otherwise is None:
            expression = f'steps["{when_name}"].outputs.parameters.outputs == "true" ? steps["{then_name}"].outputs.parameters.outputs : inputs.parameters.inputs'
        else:
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
