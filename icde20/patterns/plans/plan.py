from abc import ABC, abstractmethod


class Plan:
    @abstractmethod
    def _build_plan_from_spec(self, spec: str):
        pass
