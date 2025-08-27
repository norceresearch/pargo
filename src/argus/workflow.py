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


class Workflow(BaseModel):
    name: str
    parameters: dict[str, Any] = {}
    image: str = "python:3.11"
    schedules: list[str] | None = None
    secrets: list[str] | None = None
    _nodes: list[Node] = []
    _images: list[str] = []

    @classmethod
    def new(cls, name: str, **kwargs) -> Workflow:
        return cls(name=name, **kwargs)

    def model_post_init(self, __context):
        self._nodes.append(InitNode(task=self.parameters))
        self._images.append(self.image)

    def next(self, node: Node, image: str | None = None) -> Workflow:
        if callable(node):
            node = StepNode(task=node)
        self._nodes.append(node)
        self._images.append(image or self.image)
        return self

    def run(self):
        logger.info("Workflow started")
        for step in self._nodes:
            step.run()
        logger.info("Workflow ended")

    def to_argo(self):
        steps = []
        templates = []
        for ind, (node, image) in enumerate(zip(self._nodes, self._images)):
            s, t = node.to_argo(image, ind)
            steps.extend(s)
            templates.extend(t)

        # Remove dublicated templates and add main
        templates = [
            template
            for n, template in enumerate(templates)
            if template not in templates[:n]
        ]

        # Add secrets
        if self.secrets:
            secrets = [
                ArgoSecretRef(secretRef=ArgoParameter(name=secret))
                for secret in self.secrets
            ]
            for template in templates:
                template.script.envFrom = secrets

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
