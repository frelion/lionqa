from typing import Any

import pandas as pd

from .util import make_method


@make_method
def __lt__(x: Any, y: Any) -> Any:
    return x < y


@make_method
def __gt__(x: Any, y: Any) -> Any:
    return x > y


@make_method
def __eq__(x: Any, y: Any) -> Any:
    return x == y


@make_method
def __le__(x: Any, y: Any) -> Any:
    return x <= y


@make_method
def __ge__(x: Any, y: Any) -> Any:
    return x >= y


@make_method
def __ne__(x: Any, y: Any) -> Any:
    return x != y


@make_method
def __add__(x: Any, y: Any) -> Any:
    return x + y


@make_method
def __sub__(x: Any, y: Any) -> Any:
    return x - y


@make_method
def __mul__(x: Any, y: Any) -> Any:
    return x * y


@make_method
def __truediv__(x: Any, y: Any) -> Any:
    return x / y


@make_method
def __floordiv__(x: Any, y: Any) -> Any:
    return x // y


@make_method
def __and__(x: Any, y: Any) -> Any:
    return x & y


@make_method
def __or__(x: Any, y: Any) -> Any:
    return x | y


@make_method
def __invert__(x: Any) -> Any:
    return ~x


@make_method
def __check__(x: Any) -> bool:
    if isinstance(x, pd.Series) or isinstance(x, pd.DataFrame):
        return x.all()
    return bool(x)
