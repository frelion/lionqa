from typing import Any

from .util import make_method

import pandas as pd


@make_method
def operation(x: Any) -> Any:
    if isinstance(x, pd.Series):
        return x.isnull()
    return x is None
