from __future__ import annotations

from json import dumps, loads
from os import environ
from typing import Any, Callable

from loguru import logger

from ..argo_types.workflows import (
    ArgoParameter,
    ArgoScript,
    ArgoScriptTemplate,
    ArgoSecretRef,
    ArgoStep,
)
from .node import Node
from .run import argus_path, merge_foreach, run_foreach
from .step import StepNode, StepTask

ForeachTask = Callable[..., list[Any]]


class Foreach(Node):
    task: ForeachTask | list[Any]
    item_name: str = "item"
    image: str | None = None
    secrets: list[str] | None = None
    _then: StepNode | None = None
    _prev: str = "foreach"

    def __init__(
        self, task: ForeachTask | list[Any], item_name: str = "item", **kwargs
    ):
        super().__init__(task=task, item_name=item_name, **kwargs)

    def then(self, task: StepTask, **kwargs) -> Foreach:
        if self._prev != "foreach":
            raise RuntimeError(".then(...) must follow Foreach(...) ")
        self._then = StepNode(task=task, **kwargs)
        self._prev = "then"
        return self

    def run(self):
        logger.info("Running foreach loop")
        data_path = argus_path() / "data.json"
        if callable(self.task):
            data = loads(data_path.read_text())
            environ["ARGUS_DATA"] = dumps(data)
            items = run_foreach(self.task.__name__, self.task.__module__)
        elif isinstance(self.task, list):
            items = self.task

        results = []
        for i, item in enumerate(items):
            logger.info(f"Processing item {i}: {item}")
            environ["ARGUS_ITEM"] = dumps({self.item_name: dumps(item)})
            result = self._then.run(write_data=False)
            results.append(result)
        data_path.write_text(dumps(results))

        environ["ARGUS_DATA"] = dumps(results)
        merge_foreach()

        logger.info("Foreach loop finished")

    def to_argo(self, step_counter: int):
        steps = []
        templates = []
        if callable(self.task):
            step, template = self.to_argo_foreach(step_counter)
            steps.append(step)
            templates.append(template)

            with_param = (
                f"{{{{steps.step{step_counter}foreach.outputs.parameters.outputs}}}}"
            )

        if isinstance(self.task, list):
            with_param = dumps([dumps(task) for task in self.task])

        step, template = self._then.to_argo(step_counter, "then")
        step = step[0][0]  # unpack
        step.withParam = with_param

        # Fix item as env stuff
        step.arguments["parameters"].append(
            ArgoParameter(name="item", value="{{item}}")
        )
        template[0].script.env.append(
            ArgoParameter(
                name="ARGUS_ITEM",
                value=f'{{"{self.item_name}": "{{{{inputs.parameters.item}}}}"}}',
            )
        )
        template[0].inputs["parameters"].append(ArgoParameter(name="item"))

        templates.extend(template)
        steps.append([step])

        step, template = self.to_argo_merge(step_counter)
        steps.append(step)
        templates.append(template)

        return steps, templates

    def to_argo_foreach(self, step_counter: int):
        parameters = [
            ArgoParameter(
                name="inputs",
                value=f"{{{{steps.step{step_counter - 1}.outputs.parameters.outputs}}}}",
            )
        ]
        script_source = f'from {run_foreach.__module__} import run_foreach\nrun_foreach("{self.task.__name__}", "{self.task.__module__}")'
        step_name = f"step{step_counter}foreach"

        secrets = None
        if self.secrets:
            secrets = [
                ArgoSecretRef(secretRef=ArgoParameter(name=secret))
                for secret in self.secrets
            ]
        image_pull_policy = "Always" if self.image else None
        template = ArgoScriptTemplate(
            name=step_name,
            script=ArgoScript(
                image=self.image,
                command=["python"],
                source=script_source,
                env=[
                    ArgoParameter(
                        name="ARGUS_DATA", value="{{inputs.parameters.inputs}}"
                    ),
                    ArgoParameter(name="ARGUS_DIR", value="/tmp"),
                ],
                envFrom=secrets,
                imagePullPolicy=image_pull_policy,
            ),
            inputs={"parameters": [ArgoParameter(name="inputs")]},
            outputs={
                "parameters": [
                    ArgoParameter(
                        name="outputs", valueFrom={"path": "/tmp/foreach.json"}
                    )
                ]
            },
        )

        step = [
            ArgoStep(
                name=step_name,
                template=step_name,
                arguments={"parameters": parameters},
            )
        ]
        return step, template

    def to_argo_merge(self, step_counter: int):
        parameters = [
            ArgoParameter(
                name="inputs",
                value=f"{{{{steps.step{step_counter}then.outputs.parameters.outputs}}}}",
            )
        ]
        env = [ArgoParameter(name="ARGUS_DATA", value="{{inputs.parameters.inputs}}")]
        env.append(ArgoParameter(name="ARGUS_DIR", value="/tmp"))
        script_source = (
            f"from {merge_foreach.__module__} import merge_foreach\nmerge_foreach()"
        )
        template = ArgoScriptTemplate(
            name="foreachmerge",
            inputs={"parameters": [ArgoParameter(name="inputs")]},
            script=ArgoScript(
                image=self.image, command=["python"], source=script_source, env=env
            ),
            outputs={
                "parameters": [
                    ArgoParameter(name="outputs", valueFrom={"path": "/tmp/data.json"})
                ]
            },
        )
        step = [
            ArgoStep(
                name=f"step{step_counter}",
                template="foreachmerge",
                arguments={"parameters": parameters},
            )
        ]
        return step, template
