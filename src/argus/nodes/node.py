from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class Node(BaseModel):
    task: Any

    def get_templates(self, **kwargs) -> tuple:
        raise NotImplementedError

    def run(self):
        raise NotImplementedError
