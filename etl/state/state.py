from typing import Any, Dict
import json
from state.base_storage import BaseStorage


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл."""

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, 'w') as file:
            json.dump(state, file)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        try:
            with open(self.file_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return {}


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self.current_state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self.current_state[key] = value
        self.storage.save_state(self.current_state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        return self.current_state.get(key, None)
