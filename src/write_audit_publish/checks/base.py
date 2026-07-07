from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class CheckResult:
    check_name: str
    passed: bool
    details: str = ""


class BaseCheck(ABC):
    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def run(self, version_ref: str) -> CheckResult:
        """Execute the check against the given version reference."""
