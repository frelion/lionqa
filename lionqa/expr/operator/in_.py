from typing import Any

from .util import make_method

import pandas as pd


@make_method
def in_(x: Any, y: Any) -> Any:
    if isinstance(x, pd.Series):
        return x.isin(y)
    else:
        return x in y
