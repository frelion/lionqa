from lionqa.expr import Expr, make
from lionqa.frame import Frame
from lionqa.column import Column

import pandas as pd


class Constraint:
    pass


class Unique(Constraint):
    def __init__(self, frame: Frame, column: Column) -> None:
        super().__init__()
        self.frame = frame
        self.column = column

    def check(self) -> bool:
        mask: pd.Series = self.column.collect().duplicated()
        self.wrong_data = Expr(
            func=lambda df, mask: df[mask],
            pre=(self.frame, )
        )
        return mask.any()
