from typing import Tuple, Generator

from lionqa.partition import Partition


class DefaultPartition(Partition):
    def __init__(self) -> None:
        super().__init__()

    def __getitem__(self, index=None) -> Tuple[None, bool]:
        return None, False

    def valid(self, partition=None) -> bool:
        return True

    def iter(self) -> Generator:
        yield None
