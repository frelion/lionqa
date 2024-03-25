from typing import Tuple, Any, Generator
from abc import ABC, abstractmethod


class Partition(ABC):
    @abstractmethod
    def valid(self, partition) -> bool: ...

    @abstractmethod
    def __getitem__(self, index) -> Tuple[Any, bool]: ...

    @abstractmethod
    def iter(self) -> Generator: ...
