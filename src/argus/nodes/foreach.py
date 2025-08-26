from __future__ import annotations

from json import dumps, loads
from os import environ
from pathlib import Path
from typing import Any, Callable

from loguru import logger

from ..argo_types.workflows import (
    ArgoParameter,
    ArgoScript,
    ArgoScriptTemplate,
    ArgoStep,
)
from ..run import merge_foreach, run_foreach
from .node import Node
from .step import StepNode, StepTask

ForeachTask = Callable[..., list[Any]]


class Foreach(Node):
    task: ForeachTask | list[Any]
    task_name: str = ""
    item_name: str = "item"
    _then: StepTask | None = None
    _join: StepTask | None = None
    _prev: str = "foreach"

    def __init__(self, task: ForeachTask | list[Any], item_name: str = "item"):
        super().__init__(task=task, item_name=item_name)

    def model_post_init(self, __context):
        if callable(self.task):
            self.task_name = self.task.__name__
        else:
            self.task_name = "foreach"

    def then(self, task: StepTask) -> Foreach:
        if self._prev != "foreach":
            raise RuntimeError(".then(...) must follow Foreach(...) ")
        self._then = StepNode(task=task)
        self._prev = "then"
        return self

    def run(self):
        logger.info("Running foreach loop")
        if callable(self.task):
            data = loads(Path("/tmp/data.json").read_text())
            environ["ARGUS_DATA"] = dumps(data)
            items = run_foreach(self.task_name, self.task.__module__)
        elif isinstance(self.task, list):
            items = self.task

        results = []
        for i, item in enumerate(items):
            logger.info(f"Processing item {i}: {item}")
            environ["ARGUS_ITEM"] = dumps({self.item_name: item})
            result = self._then.run(write_data=False)
            results.append(result)
        Path("/tmp/data.json").write_text(dumps(results))

        environ["ARGUS_DATA"] = dumps(results)
        merge_foreach()

        logger.info("Foreach loop finished")

    def to_argo(self, image: str, step_counter: int):
        parameters = [
            ArgoParameter(
                name="inputs",
                value=f"{{{{steps.step{step_counter - 1}.outputs.parameters.outputs}}}}",
            )
        ]

        steps = []
        templates = []
        if callable(self.task):
            script_source = f'from {run_foreach.__module__} import run_foreach\nrun_foreach("{self.task_name}", "{self.task.__module__}")'
            step_name = f"step{step_counter}foreach"

            templates.append(
                ArgoScriptTemplate(
                    name=self.task_name,
                    script=ArgoScript(
                        image=image,
                        command=["python"],
                        source=script_source,
                        env=[
                            ArgoParameter(
                                name="ARGUS_DATA", value="{{inputs.parameters.inputs}}"
                            )
                        ],
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
            )

            steps.append(
                [
                    ArgoStep(
                        name=step_name,
                        template=self.task_name,
                        arguments={"parameters": parameters},
                    )
                ]
            )
            with_items = f"{{{{steps.{step_name}.outputs.parameters.outputs}}}}"
            step, template = self._then.to_argo(image, step_counter, "then")
            step = step[0][0]  # unpack
            step.withParam = with_items

        if isinstance(self.task, list):
            with_items = self.task
            step, template = self._then.to_argo(image, step_counter, "then")
            step = step[0][0]  # unpack
            step.withItems = with_items

        templates.extend(template)
        steps.append([step])

        # Merge
        parameters = [
            ArgoParameter(
                name="inputs",
                value=f"{{{{steps.step{step_counter}then.outputs.parameters.outputs}}}}",
            )
        ]
        env = [ArgoParameter(name="ARGUS_DATA", value="{{inputs.parameters.inputs}}")]
        script_source = (
            f"from {merge_foreach.__module__} import merge_foreach\nmerge_foreach()"
        )
        templates.append(
            ArgoScriptTemplate(
                name="foreachmerge",
                inputs={"parameters": [ArgoParameter(name="inputs")]},
                script=ArgoScript(
                    image=image, command=["python"], source=script_source, env=env
                ),
                outputs={
                    "parameters": [
                        ArgoParameter(
                            name="outputs", valueFrom={"path": "/tmp/data.json"}
                        )
                    ]
                },
            )
        )
        steps.append(
            [
                ArgoStep(
                    name=f"step{step_counter}",
                    template="foreachmerge",
                    arguments={"parameters": parameters},
                )
            ]
        )

        return steps, templates

    def to_argo_foreach(self, image: str, step_counter: int):
        pass

    def to_argo_then(self, image: str, step_counter: int):
        pass

    def to_argo_merge(self, image: str, step_counter: int):
        pass
