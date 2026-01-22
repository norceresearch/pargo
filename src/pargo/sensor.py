from __future__ import annotations

from json import dumps
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from yaml import safe_dump

from .argo_types.events import (
    ArgoWorkflow,
    Dependency,
    EventSensor,
    EventSpec,
    EventTemplate,
    FilterData,
    Filters,
    Source,
    Trigger,
    TriggerTemplate,
)
from .argo_types.primitives import Metadata, TemplateRef
from .argo_types.workflows import WorkflowResource, WorkflowSpec

if TYPE_CHECKING:
    from .workflow import Condition


class Sensor(BaseModel):
    name: str
    trigger_on: Condition
    parameters: list[dict[str, Any]] | None = None

    def argo_dependencies(self):
        dependencies = []
        for name in self.trigger_on.names:
            dependencies.append(
                Dependency(
                    name=name,
                    eventSourceName="argo-workflow-events",
                    eventName="workflow-events",
                    filters=Filters(
                        data=[
                            FilterData(
                                path="body.metadata.name",
                                type="string",
                                value=[f"^{name}-.*"],
                            ),
                            FilterData(
                                path="body.status.phase",
                                type="string",
                                value=["Succeeded"],
                            ),
                        ]
                    ),
                )
            )
        return dependencies

    def argo_triggers(self):
        if self.parameters:
            arguments = []
            for parameters in self.parameters:
                arguments.append(
                    {
                        "parameters": [
                            {"name": k, "value": dumps(v)}
                            for k, v in parameters.items()
                        ]
                    },
                )
        else:
            arguments = [None] * len(self.trigger_on)

        triggers = []
        for ind, (condition, argument) in enumerate(
            zip(self.trigger_on.items, arguments)
        ):
            triggers.append(
                Trigger(
                    template=TriggerTemplate(
                        name=self.name + str(ind),
                        conditions=condition,
                        argoWorkflow=ArgoWorkflow(
                            group="argoproj.io",
                            version="v1alpha1",
                            resource="workflows",
                            operation="submit",
                            source=Source(
                                resource=WorkflowResource(
                                    apiVersion="argoproj.io/v1alpha1",
                                    kind="Workflow",
                                    metadata=Metadata(
                                        generateName=f"{self.name}-",
                                        namespace="argo-workflows",
                                    ),
                                    spec=WorkflowSpec(
                                        workflowTemplateRef=TemplateRef(name=self.name),
                                        arguments=argument,
                                    ),
                                )
                            ),
                        ),
                    )
                )
            )
        return triggers

    def to_argo(self):
        sensor = EventSensor(
            apiVersion="argoproj.io/v1alpha1",
            kind="Sensor",
            metadata=Metadata(name=self.name),
            spec=EventSpec(
                eventBusName="argoevents",
                template=EventTemplate(serviceAccountName="argo-service-account"),
                dependencies=self.argo_dependencies(),
                triggers=self.argo_triggers(),
            ),
        )
        return sensor

    def to_yaml(self, path=""):
        sensor = self.to_argo()
        yaml_str = sensor.model_dump(exclude_none=True)
        Path(path / (self.name + "-sensor.yaml")).write_text(
            safe_dump(yaml_str, sort_keys=False),
            encoding="utf-8",
            newline="\n",
        )
