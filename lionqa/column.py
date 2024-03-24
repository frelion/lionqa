from typing import Optional, List

import lionqa
from lionqa import Expr


class Column(Expr):
    __slots__ = [
        "frame_name", "column_name", "func", "pre", "constraints"
    ]

    def __init__(
        self, 
        column_name: Optional[str]=None,
        frame_name: Optional[str]=None,
        constraints: Optional[List]=None,
    ) -> None:
        self.frame_name = frame_name
        self.column_name = column_name
        self.constraints = constraints
        super().__init__()

    def set(self, frame: "lionqa.Frame"):
        name = ""
        if self.frame_name is not None:
            name += self.frame_name+"."
        if self.column_name is None:
            raise ValueError("The column is undefined")
        name += self.column_name
        self.func = lambda series: series
        self.pre = (frame[name], )

    def clone(self, clonespace: dict):
        if id(self) not in clonespace:
            _new =  Column(self.column_name, self.frame_name)
            _new.func = self.func
            _new.pre = tuple(
                pre.clone(clonespace)
                for pre in self.pre
            )
            clonespace[id(self)] = _new
        return clonespace[id(self)]

