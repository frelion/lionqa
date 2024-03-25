from abc import ABC, abstractmethod


class Partition(ABC):
    @abstractmethod
    def __contains__(self, other) -> bool: ...

    @abstractmethod
    def __getitem__(self, index) -> bool: ...
