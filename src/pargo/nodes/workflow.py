from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import Field
from yaml import dump

from ..argo_types.primitives import Metadata, TemplateRef
from ..argo_types.workflows import (
    Parameter,
    Resource,
    ResourceTemplate,
    RetryStrategy,
    StepsTemplate,
    Task,
    WorkflowResource,
    WorkflowSpec,
)
from .node import Node

if TYPE_CHECKING:
    from ..workflow import Workflow

WorkflowTask = list["Workflow"]


class WorkflowNode(Node):
    """Class for launching other workflows."""

    task: WorkflowTask = Field(description="Workflow to trigger")

    @property
    def argo_name(self):
        """Name of the task."""
        return "workflow"

    def run(self, workflow_name: str | None = None):
        """Run the step locally"""
        for workflow in self.task:
            workflow.run()

    def get_templates(
        self,
        step_counter: int,
        default_image: str,
        image_pull_policy: str,
        default_secrets: list[str] | None,
        default_parameters: dict[str, Any],
        default_retry: int | RetryStrategy | None,
    ):
        """Returns a list with workflow reference templates @private"""
        block_name = f"step-{step_counter}-{self.argo_name}"

        templates = [self._get_steps(block_name, default_parameters)]
        for workflow in self.task:
            template_name = block_name + "-" + workflow.name

            child = WorkflowResource(
                kind="Workflow",
                metadata=Metadata(generateName=workflow.name + "-"),
                spec=WorkflowSpec(workflowTemplateRef=TemplateRef(name=workflow.name)),
            )

            resource = Resource(
                manifest=dump(child.model_dump(exclude_none=True), sort_keys=False)
            )

            template = ResourceTemplate(
                name=template_name,
                resource=resource,
            )
            templates.append(template)

        return templates

    def _get_steps(self, block_name: str, default_parameters: dict[str, Any]):
        parallel_steps = []
        for workflow in self.task:
            name = block_name + "-" + workflow.name
            parallel_steps.append(Task(name=name, template=name))

        default = ",".join(
            f'"{k}": {{{{workflow.parameters.{k}}}}}' for k in default_parameters
        )
        default = f"{{{default}}}"

        steps = StepsTemplate(
            name=block_name,
            inputs={"parameters": [Parameter(name="inputs", default=default)]},
            steps=[parallel_steps],
            outputs={
                "parameters": [
                    Parameter(
                        name="outputs",
                        valueFrom={"parameter":"{{inputs.parameters.inputs}}"},
                    ),
                ]
            },
        )
        return steps
