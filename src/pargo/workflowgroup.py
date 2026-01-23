from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field
from yaml import safe_dump

from .argo_types.primitives import (
    Metadata,
    PodMetadata,
    TemplateRef,
)
from .argo_types.workflows import (
    StepsTemplate,
    Task,
    WorkflowResource,
    WorkflowSpec,
)
from .workflow import Workflow


class WorkflowGroup(BaseModel):
    """
    Class for creating groups of Workflows to be triggered in parallel remotely.
    """

    name: str = Field(
        description="Name of the workflow",
        pattern=r"^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$",
        max_length=63,
    )

    parallelism: int = Field(
        None,
        description="Maximum number of parallel containers running at the same time",
    )
    pod_metadata: None | PodMetadata = Field(default=None, description="")

    _workflows: list[list[Workflow]] = []
    _annotations = __annotations__

    @classmethod
    def new(cls, name: str, **kwargs) -> Workflow:
        """
        Create a new `WorkflowGroup` instance using `WorkflowGroup.new(name="myworkflow")`.
        """
        return cls(name=name, **kwargs)

    def next(self, workflows=Workflow | list[Workflow]):
        self._workflows.append(
            [workflows] if isinstance(workflows, Workflow) else workflows
        )
        return self

    def run(self):
        for workflows in self._workflows:
            for workflow in workflows:
                workflow.run()

    def to_argo(self):
        steps = StepsTemplate(name="main", steps=[])
        for ind, workflows in enumerate(self._workflows):
            step = []
            for workflow in workflows:
                step.append(
                    Task(
                        name=f"step-{ind}-{workflow.name}",
                        templateRef=TemplateRef(name=workflow.name, template="main"),
                    )
                )
            steps.steps.append(step)

        spec = WorkflowSpec(
            entrypoint="main",
            templates=[steps],
            parallelism=self.parallelism,
        )

        wf = WorkflowResource(
            kind="WorkflowTemplate",
            metadata=Metadata(name=self.name),
            spec=spec,
        )
        return wf

    def to_yaml(self, path: Path | str = ""):
        """Write manifest to run the workflowgroup on Argo Workflows."""
        if isinstance(path, str):
            path = Path(path)
        wf = self.to_argo()
        yaml_str = wf.model_dump(exclude_none=True)
        Path(path / (self.name + ".yaml")).write_text(
            safe_dump(yaml_str, sort_keys=False),
            encoding="utf-8",
            newline="\n",
        )

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
