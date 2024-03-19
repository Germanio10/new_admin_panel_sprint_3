import abc
import json
from typing import Any, Dict


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """
    MAX_RETRIES = 5

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any], retries: int = 0) -> None:
        """Сохранить состояние в хранилище."""
        try:
            with open(self.file_path, "r+") as file:
                saved_state = json.load(file)
                saved_state.update(**state)
                file.seek(0)
                json.dump(saved_state, file, indent=4)
                file.truncate()
        except FileNotFoundError:
            self.create_state_file(retries)
            retries += 1
            self.save_state(state, retries)

    def retrieve_state(self, retries: int = 0) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        try:
            with open(self.file_path, "r") as file:
                state = json.load(file)
            return state
        except FileNotFoundError:
            self.create_state_file(retries)
            retries += 1
            self.retrieve_state(retries)

    def create_state_file(self, retries: int) -> None:
        """Создать файл для хранения состояние, если его не существовало"""
        if retries > self.MAX_RETRIES:
            raise Exception("Can not create state file in %s" % self.file_path)

        with open(self.file_path, "w") as file:
            json.dump({}, file)


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        state = {
            key: value,
        }
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        state = self.storage.retrieve_state()
        return state[key] if state and key in state else None
