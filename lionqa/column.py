from typing import Optional, List

import lionqa
from lionqa import Expr


class Column(Expr):
    """数据列的抽象"""

    __slots__ = ["frame_name", "column_name", "func", "pre", "constraints"]

    def __init__(
        self,
        column_name: Optional[str] = None,
        frame_name: Optional[str] = None,
        constraints: Optional[
            List["lionqa.Constraint"]
        ] = None,  # column不对其进行检测。其作用仅是向frame提供信息
    ) -> None:
        self.frame_name = frame_name
        self.column_name = column_name
        self.constraints = constraints or list()
        for cons in self.constraints:
            if not isinstance(cons, lionqa.Constraint):
                raise TypeError(f"{self}'s constraints {cons} is not Constraint type")
        super().__init__()

    def _clone(self, clonespace: dict) -> "Column":
        _new = Column(self.column_name, self.frame_name, self.constraints)
        _new.func = self.func
        _new.pre = tuple(pre.clone(clonespace) for pre in self.pre)
        return _new

    def _bind(self, frame: "lionqa.Frame"):
        if self.column_name is None:
            raise ValueError("The column is undefined")
        self.func = lambda series: series
        self.pre = (frame[(self.frame_name, self.column_name)],)
