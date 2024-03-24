from abc import ABC, abstractmethod


class Partition:
    @abstractmethod
    def __contains__(self, other) -> bool:
        ...
    
    @abstractmethod
    def get(self, index) -> bool:
        ...

