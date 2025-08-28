from pydantic import BaseModel


class ArgoWorkflowTemplateRef(BaseModel):
    name: str


class ArgoWorkflowSpec(BaseModel):
    workflowTemplateRef: ArgoWorkflowTemplateRef


class ArgoWorkflowMetadata(BaseModel):
    generateName: str | None = None
    namespace: str | None = None


class ArgoWorkflowResource(BaseModel):
    apiVersion: str
    kind: str
    metadata: ArgoWorkflowMetadata
    spec: ArgoWorkflowSpec


class ArgoWorkflowSource(BaseModel):
    resource: ArgoWorkflowResource


class ArgoWorkflowTrigger(BaseModel):
    group: str
    version: str
    resource: str
    operation: str
    source: ArgoWorkflowSource


class ArgoTriggerTemplate(BaseModel):
    name: str
    conditions: str | None = None
    argoWorkflow: ArgoWorkflowTrigger


class ArgoTrigger(BaseModel):
    template: ArgoTriggerTemplate


class ArgoFilterData(BaseModel):
    path: str
    type: str
    value: list[str]


class ArgoFilters(BaseModel):
    data: list[ArgoFilterData]


class ArgoDependency(BaseModel):
    name: str
    eventSourceName: str
    eventName: str
    filters: ArgoFilters | None = None


class ArgoTemplate(BaseModel):
    serviceAccountName: str


class ArgoSpec(BaseModel):
    eventBusName: str
    template: ArgoTemplate
    dependencies: list[ArgoDependency]
    triggers: list[ArgoTrigger]


class ArgoMetadata(BaseModel):
    name: str
    namespace: str | None = None


class ArgoSensor(BaseModel):
    apiVersion: str
    kind: str = "Sensor"
    metadata: ArgoMetadata
    spec: ArgoSpec
