"""
Path configuration.

Replaces the original exercise's hardcoded `abfss://` course-storage
URLs and the `student_name` collision-avoidance variable. Paths are
resolved from the DATA_ROOT environment variable, so the same code
runs against a local directory, a Codespaces workspace, or a
Databricks Unity Catalog Volume with no edits:

    DATA_ROOT=./data                              # local
    DATA_ROOT=/Volumes/workspace/accidents/data   # Databricks
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Paths:
    root: str

    @classmethod
    def from_env(cls) -> "Paths":
        return cls(root=os.environ.get("DATA_ROOT", "./data"))

    @property
    def raw(self) -> str:
        return f"{self.root}/raw"

    @property
    def out(self) -> str:
        return f"{self.root}/out"

    def output(self, fmt: str) -> str:
        """Output path for a given storage format, e.g. 'csv'."""
        return f"{self.out}/accidents_{fmt}"
