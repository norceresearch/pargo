from pathlib import Path

from pydantic import BaseModel
from yaml import safe_dump

from .argo_types.events import (
    ArgoDependency,
    ArgoFilterData,
    ArgoFilters,
    ArgoMetadata,
    ArgoSensor,
    ArgoSpec,
    ArgoTemplate,
    ArgoTrigger,
    ArgoTriggerTemplate,
    ArgoWorkflowMetadata,
    ArgoWorkflowResource,
    ArgoWorkflowSource,
    ArgoWorkflowSpec,
    ArgoWorkflowTemplateRef,
    ArgoWorkflowTrigger,
)


class TriggerOn(BaseModel):
    flows: list[str]


class Sensor(BaseModel):
    name: str
    trigger_on: str | list[str] | list[TriggerOn]

    def model_post_init(self, __context):
        if isinstance(self.trigger_on, str):
            self.trigger_on = [TriggerOn(flows=[self.trigger_on])]
        elif isinstance(self.trigger_on, list):
            self.trigger_on = [TriggerOn(flows=self.trigger_on)]

    def argo_dependencies(self):
        dependency_names = [flow for t in self.trigger_on for flow in t.flows]
        dependency_names = list(set(dependency_names))  # remove dublicates
        dependencies = []
        for name in dependency_names:
            dependencies.append(
                ArgoDependency(
                    name=name,
                    eventSourceName="argo-workflow-events",
                    eventName="workflow-events",
                    filters=ArgoFilters(
                        data=[
                            ArgoFilterData(
                                path="body.metadata.generateName",
                                type="string",
                                value=[f"{name}-"],
                            ),
                            ArgoFilterData(
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
        conditions = [" && ".join(t.flows) for t in self.trigger_on]
        triggers = []
        for condition in conditions:
            triggers.append(
                ArgoTrigger(
                    template=ArgoTriggerTemplate(
                        name=self.name,
                        conditions=condition,
                        argoWorkflow=ArgoWorkflowTrigger(
                            group="argoproj.io",
                            version="v1alpha1",
                            resource="workflows",
                            operation="submit",
                            source=ArgoWorkflowSource(
                                resource=ArgoWorkflowResource(
                                    apiVersion="argoproj.io/v1alpha1",
                                    kind="Workflow",
                                    metadata=ArgoWorkflowMetadata(
                                        generateName=f"{self.name}-",
                                        namespace="argo-workflows",
                                    ),
                                    spec=ArgoWorkflowSpec(
                                        workflowTemplateRef=ArgoWorkflowTemplateRef(
                                            name=self.name
                                        )
                                    ),
                                )
                            ),
                        ),
                    )
                )
            )
        return triggers

    def to_argo(self):
        sensor = ArgoSensor(
            apiVersion="argoproj.io/v1alpha1",
            kind="Sensor",
            metadata=ArgoMetadata(name=self.name, namespace="argo-workflows"),
            spec=ArgoSpec(
                eventBusName="argoevents",
                template=ArgoTemplate(serviceAccountName="argo-service-account"),
                dependencies=self.argo_dependencies(),
                triggers=self.argo_triggers(),
            ),
        )
        return sensor

    def to_yaml(self, path=""):
        sensor = self.to_argo()
        yaml_str = sensor.model_dump(exclude_none=True)
        Path(path / (self.name + "-sensor.yaml")).write_text(
            safe_dump(yaml_str, sort_keys=False)
        )
