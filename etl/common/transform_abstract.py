import abc


class TransformAbstract(abc.ABC):
    @abc.abstractmethod
    def transform(self) -> list[list]:
        pass

    @abc.abstractmethod
    def actions_list(self):
        pass
