from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class Node(BaseModel):
    task: Any

    def to_argo(self, **kwargs) -> tuple:
        raise NotImplementedError

    def run(self):
        raise NotImplementedError
