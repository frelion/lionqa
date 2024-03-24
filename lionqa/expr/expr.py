from typing import Optional, Callable, Sequence, Generator, Any


class Expr:
    __slots__ = [
        "pre", "func", "_collected", "_value"
    ]

    def __init__(
        self, 
        func: Optional[Callable]=None, # 一个无状态函数，必须是无状态的
        pre: Sequence["Expr"] = tuple(),
    ) -> None:
        self.func = func
        self.pre = tuple(pre)
        self._collected = False
        self._value = None

    def _collect(self) -> Any:
        if not self._collected:
            print(self, "calculate")
            if self.func is None:
                raise ValueError("数据源尚未定义")
            self._value = self.func(*(
                pre._collect()
                for pre in self.pre
            ))
            self._collected = True
        return self._value

    def clone(self, clonespace: dict) -> "Expr":
        if id(self) not in clonespace:
            clonespace[id(self)] = Expr(
                self.func,
                tuple(
                    pre.clone(clonespace)
                    for pre in self.pre
                )
            )
        return clonespace[id(self)]

    def collect(self) -> Any:
        expr = self.clone(dict())
        return expr._collect()

    def roots(self) -> Generator["Expr", None, None]:
        if not self.pre:
            yield self
        for pre in self.pre:
            yield from pre.roots()

    def check(self) -> bool:
        return self.__check__().collect()
