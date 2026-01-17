from typing import Any


class Stateful:
    def get_state(self) -> dict[str, Any]:
        return {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        }

    def set_state(self, state: dict[str, Any]) -> None:
        for k, v in state.items():
            setattr(self, k, v)
