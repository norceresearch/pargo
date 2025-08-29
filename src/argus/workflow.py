from __future__ import annotations

from json import dumps
from pathlib import Path
from typing import Any

from loguru import logger
from pydantic import BaseModel
from yaml import safe_dump

from .argo_types.workflows import (
    ArgoCronWorkflow,
    ArgoCronWorkflowSpec,
    ArgoParameter,
    ArgoPodGC,
    ArgoScriptTemplate,
    ArgoSecretRef,
    ArgoStepsTemplate,
    ArgoTTLStrategy,
    ArgoWorkflow,
    ArgoWorkflowMetadata,
    ArgoWorkflowSpec,
    ArgoWorkflowTemplateRef,
)
from .nodes.init import InitNode
from .nodes.node import Node
from .nodes.step import StepNode
from .sensor import Sensor
from .trigger_condition import Condition


class Workflow(BaseModel):
    name: str
    parameters: dict[str, Any] = {}
    image: str = "python:3.11"
    schedules: list[str] | None = None
    secrets: list[str] | None = None
    trigger_on: Workflow | Condition = None
    trigger_on_parameters: list[dict[str, Any]] | None = None
    _nodes: list[Node] = []

    @classmethod
    def new(cls, name: str, **kwargs) -> Workflow:
        return cls(name=name, **kwargs)

    def model_post_init(self, __context):
        self._nodes.append(InitNode(task=self.parameters))

        if isinstance(self.trigger_on, Workflow):
            self.trigger_on = Condition(items=[self.trigger_on.name])

        if self.trigger_on_parameters:
            if len(self.trigger_on_parameters) != len(self.trigger_on):
                raise ValueError(
                    "trigger_on_parameters must be same length as number of OR statements when defined."
                )

    def next(self, node: Node, **kwargs) -> Workflow:
        if callable(node):
            node = StepNode(task=node, **kwargs)
        self._nodes.append(node)
        return self

    def run(self):
        logger.info("Workflow started")
        for step in self._nodes:
            step.run()
        logger.info("Workflow ended")

    def to_argo(self):
        steps = []
        templates = []
        for ind, node in enumerate(self._nodes):
            s, t = node.to_argo(ind)
            steps.extend(s)
            templates.extend(t)

        templates = self._remove_duplicated_templates(templates)
        self._add_default_image(templates)
        self._add_default_secrets(templates)

        templates = [ArgoStepsTemplate(name="main", steps=steps)] + templates

        spec = ArgoWorkflowSpec(
            entrypoint="main",
            arguments={
                "parameters": [
                    {"name": k, "value": dumps(v), "default": dumps(v)}
                    for k, v in self.parameters.items()
                ]
            },
            templates=templates,
            schedules=self.schedules,
            ttlStrategy=ArgoTTLStrategy(),
            podGC=ArgoPodGC(),
        )

        wf = ArgoWorkflow(
            kind="WorkflowTemplate",
            metadata=ArgoWorkflowMetadata(name=self.name),
            spec=spec,
        )
        return wf

    def to_yaml(self, path=""):
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

    def to_yaml_cron(self, path):
        wf = ArgoCronWorkflow(
            kind="CronWorkflowTemplate",
            metadata=ArgoWorkflowMetadata(name=self.name),
            spec=ArgoCronWorkflowSpec(
                schedules=self.schedules,
                workflowSpec=ArgoWorkflowTemplateRef(
                    workflowTemplateRef=ArgoParameter(name=self.name)
                ),
            ),
        )
        yaml_str = wf.model_dump(exclude_none=True)
        Path(path / (self.name + "-cron.yaml")).write_text(
            safe_dump(yaml_str, sort_keys=False)
        )

    @staticmethod
    def _remove_duplicated_templates(
        templates: list[ArgoScriptTemplate],
    ) -> list[ArgoScriptTemplate]:
        templates = [
            template
            for n, template in enumerate(templates)
            if template not in templates[:n]
        ]
        template_names = [template.name for template in templates]
        if len(template_names) > len(set(template_names)):
            raise RuntimeError(
                "Duplicate task detected: The same task is provided with different kwargs (image, secrets etc)."
            )
        return templates

    def _add_default_image(self, templates: list[ArgoScriptTemplate]):
        for template in templates:
            template.script.image = template.script.image or self.image

    def _add_default_secrets(self, templates: list[ArgoScriptTemplate]):
        secrets = None
        if self.secrets:
            secrets = [
                ArgoSecretRef(secretRef=ArgoParameter(name=secret))
                for secret in self.secrets
            ]
        for template in templates:
            template.script.envFrom = template.script.envFrom or secrets

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


# Rebuilding the pydantic model after Workflow is defined
Condition.model_rebuild()
Sensor.model_rebuild()
