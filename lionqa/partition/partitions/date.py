import datetime as dt
from typing import Tuple, Union, Optional, Generator

from lionqa.partition import Partition


class DatePartition(Partition):
    def __init__(
        self, start: Optional[dt.datetime], end: Optional[dt.datetime], desc=True
    ) -> None:
        super().__init__()
        self.start = start or dt.datetime.min
        self.end = end or dt.datetime.max
        self.desc = desc

    def __getitem__(self, index: Union[dt.datetime, int]) -> Tuple[dt.datetime, bool]:
        if isinstance(index, int):
            return dt.timedelta(days=index), True
        elif isinstance(index, dt.datetime):
            index = dt.datetime.strptime(index.strftime("%Y-%m-%d"), "%Y-%m-%d")
            if index > self.end or index < self.start:
                raise ValueError(f"{index} out of range [{self.start}, {self.end}]")
            else:
                return index, False
        else:
            raise TypeError(f"{type(index)} not support, must be of datetime or int")

    def valid(self, partition: dt.datetime) -> bool:
        if not isinstance(partition, dt.datetime):
            # raise TypeError(
            #     f"partition must be type of datetime, not {type(partition)}"
            # )
            return False
        return partition <= self.end and partition >= self.start

    def iter(self) -> Generator:
        if self.desc:
            cur = self.start
            while cur <= self.end:
                yield cur
                cur += dt.timedelta(days=1)
        else:
            cur = self.end
            while cur >= self.start:
                yield cur
                cur -= dt.timedelta(days=1)
