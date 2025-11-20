from __future__ import annotations

from copy import deepcopy
from json import dumps
from pathlib import Path
from typing import Any, Callable, Literal

from loguru import logger
from pydantic import BaseModel, Field
from yaml import safe_dump

from .argo_types.cron import (
    CronWorkflow,
    CronWorkflowSpec,
)
from .argo_types.primitives import (
    Metadata,
    Parameter,
    PodGC,
    PodMetadata,
    RetryStrategy,
    TemplateRef,
    TTLStrategy,
)
from .argo_types.workflows import (
    StepsTemplate,
    Task,
    WorkflowResource,
    WorkflowSpec,
)
from .nodes.node import Node
from .nodes.run import argus_path
from .nodes.step import StepNode
from .sensor import Sensor
from .trigger_condition import Condition


class Workflow(BaseModel):
    """
    Class for creating Argus workflows.
    """

    name: str = Field(
        description="Name of the workflow",
        example="test-workflow",
        pattern=r"^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$",
        max_length=63,
    )
    parameters: dict[str, Any] = Field(
        default={},
        description="Named parameters that are available for the workflow tasks.",
        example={"my_int": 3},
    )
    image: str = Field(
        default="python:3.11",  # FIXME Don't make sense as argus is not in this image
        description="Name of image to pull.",
    )
    image_pull_policy: Literal["Always", "IfNotPresent", "Never", None] = Field(
        default="Always", description="Pull policy for `image`."
    )
    schedules: list[str] | None = Field(
        default=None,
        description="Set scheduled execution. Creates an additional cron-manifest when provided.",
        example=["0 0 * * *"],
    )
    secrets: list[str] | None = Field(default=None, description="")
    trigger_on: Workflow | Condition | None = Field(
        default=None,
        description="Set triggered execution by providing upstream workflow(s) this workflow depends on. Creates an additional sensor-manifest when provided.",
        example="trigger_on=workflow1 | workflow2 & workflow3",
    )
    trigger_on_parameters: list[dict[str, Any]] | None = Field(
        default=None,
        description="Input parameters to the workflow when triggered by upstream workflows. Must match the length of `trigger_on`",
    )
    parallelism: int | None = Field(
        default=None,
        description="Maximum number of parallel containers running at the same time. Default (None) uses the maximum set by the service.",
    )
    pod_metadata: None | PodMetadata = Field(default=None, description="")
    retry: int | RetryStrategy | None = Field(
        default=2, description="Set the number of retries or the full retry strategy."
    )
    _nodes: list[Node] = []

    _annotations = __annotations__

    @classmethod
    def new(cls, name: str, **kwargs) -> Workflow:
        """
        Create a new `Workflow` instance using `Workflow.new(name="myworkflow")`.
        """
        return cls(name=name, **kwargs)

    def model_post_init(self, __context):
        if isinstance(self.trigger_on, Workflow):
            self.trigger_on = Condition(items=[self.trigger_on.name])

        if self.trigger_on_parameters:
            if len(self.trigger_on_parameters) != len(self.trigger_on):
                raise ValueError(
                    "trigger_on_parameters must be same length as number of OR statements when defined."
                )

    def next(self, node: Node | Callable, **kwargs) -> Workflow:
        """Add tasks or Nodes to the workflow. Callable tasks are converted to StepNodes."""
        if callable(node):
            node = StepNode(task=node, **kwargs)
        self._nodes.append(node)
        return self

    def run(self, parameters: dict[str, Any] | None = None):
        """Run the workflow locally."""
        logger.info(f"Workflow {self.name} started")

        defaults = deepcopy(self.parameters)
        if parameters:  # Override default parameters
            defaults.update(
                (k, parameters[k]) for k in defaults.keys() & parameters.keys()
            )
        data_path = argus_path() / "data.json"
        data_path.write_text(dumps(defaults))

        for step in self._nodes:
            step.run()
        logger.info("Workflow ended")

    def to_argo(self):
        """Generate a pydantic model of the workflow."""
        steps = StepsTemplate(name="main", steps=[])
        arguments = None
        templates = []
        for ind, node in enumerate(self._nodes):
            t = node.get_templates(
                step_counter=ind,
                default_image=self.image,
                image_pull_policy=self.image_pull_policy,
                default_secrets=self.secrets,
                default_parameters=self.parameters,
                default_retry=self.retry,
            )
            s = Task(
                name=f"step-{ind}-{node.argo_name}",
                template=f"step-{ind}-{node.argo_name}",
                arguments=arguments,
            )
            steps.steps.append([s])
            templates.extend(t)
            arguments = self._next_argument(ind, node.argo_name)

        spec = WorkflowSpec(
            entrypoint="main",
            arguments={
                "parameters": [
                    {"name": k, "value": dumps(v), "default": dumps(v)}
                    for k, v in self.parameters.items()
                ]
            },
            templates=[steps] + templates,
            ttlStrategy=TTLStrategy(),
            podGC=PodGC(),
            parallelism=self.parallelism,
            podMetadata=self.pod_metadata,
        )

        wf = WorkflowResource(
            kind="WorkflowTemplate",
            metadata=Metadata(name=self.name),
            spec=spec,
        )
        return wf

    def to_yaml(self, path: Path | str = ""):  # FIXME write/dump ?
        """Write manifest(s) to run the workflow on Argo Workflows."""
        if isinstance(path, str):
            path = Path(path)
        wf = self.to_argo()
        yaml_str = wf.model_dump(exclude_none=True)
        Path(path / (self.name + ".yaml")).write_text(
            safe_dump(yaml_str, sort_keys=False)
        )

        if self.schedules:
            self.to_yaml_cron(path=path)

        if self.trigger_on:
            sensor = Sensor(
                name=self.name,
                trigger_on=self.trigger_on,
                parameters=self.trigger_on_parameters,
            )
            sensor.to_yaml(path=path)

    def to_yaml_cron(self, path):  # FIXME write_cron_yaml/manifest?
        """Write manifest for scheduled execution on Argo Workflows."""
        wf = CronWorkflow(
            metadata=Metadata(name=self.name),
            spec=CronWorkflowSpec(
                schedules=self.schedules,
                workflowSpec=WorkflowSpec(
                    workflowTemplateRef=TemplateRef(name=self.name)
                ),
            ),
        )
        yaml_str = wf.model_dump(exclude_none=True)
        Path(path / (self.name + "-cron.yaml")).write_text(
            safe_dump(yaml_str, sort_keys=False)
        )

    @staticmethod
    def _next_argument(ind: int, name: str):
        parameters = [
            Parameter(
                name="inputs",
                value=f"{{{{steps.step-{ind}-{name}.outputs.parameters.outputs}}}}",
            )
        ]
        return {"parameters": parameters}

    def __and__(self, other):
        if isinstance(other, Workflow):
            if self.name == other.name:
                return Condition(items=[self.name])
            return Condition(items=[f"{self.name} && {other.name}"])
        else:
            if len(other.items) > 1:
                raise ValueError("Invalid: cannot do (A | B) & C")
            return Condition(items=[f"{self.name} && {other.items[0]}"])

    def __or__(self, other):
        if isinstance(other, Workflow):
            if self.name == other.name:
                return Condition(items=[self.name])
            return Condition(items=[self.name, other.name])
        else:
            return Condition(items=[self.name] + other.items)

    def __repr__(self):
        return f"{self.__class__.__name__}<name={self.name}>"

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return hash(
            getattr(self, attr)
            for attr in self._annotations
            if not isinstance(getattr(self, attr), dict)
        )


# Rebuilding the pydantic model after Workflow is defined
Condition.model_rebuild()
Sensor.model_rebuild()
