from typing import Any

from lionqa.expr import Expr


def make(value: Any) -> Expr:
    return Expr(func=lambda: value)

def make_method(func):
    def inner(self: Expr, *args) -> Expr:
        args = tuple(
            arg if isinstance(arg, Expr) else make(arg)
            for arg in args
        )
        return Expr(
            func=func,
            pre=(self, *args)
        )
    setattr(Expr, func.__name__, inner)
    return inner
