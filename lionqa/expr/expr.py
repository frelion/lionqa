import inspect
from typing import Optional, Callable, Sequence, Callable, Any

import lionqa


def get_signature(func: Callable):
    return (
        func.__name__,
        inspect.signature(func).parameters,
        inspect.signature(func).return_annotation,
    )


class Expr:
    """
    核心类，用于构建计算逻辑

    每一个Expr为一个节点
    多个Expr节点构成的计算图仅可计算一次
    """

    __slots__ = ["pre", "func", "_collected", "_value"]

    def __init_subclass__(cls, **kwargs) -> None:
        """仅对直接继承的类进行检查"""
        if Expr not in cls.__bases__:
            return
        if cls._clone is Expr._clone:
            raise NotImplementedError(f"class {cls} must implement '_clone' method")
        _, params, return_annotation = get_signature(cls._clone)
        if list(params) != ["self", "clonespace"] or (
            return_annotation is not cls and return_annotation != cls.__name__
        ):
            raise NotImplementedError(
                "\n".join(
                    (
                        f"class {cls}'s method '_clone' must be an instance method, ",
                        "and the parameters must be ['self', 'clonespace'] where clonespace is a dict used for cache, ",
                        f"and the return type must be {cls} (please write return type annotation)",
                    )
                )
            )
        if cls._bind is Expr._bind:
            raise NotImplementedError(f"class {cls} must implement '_bind' method")
        _, params, return_annotation = get_signature(cls._bind)
        if list(params) != ["self", "frame"] or (
            return_annotation is not None and return_annotation is not inspect._empty
        ):
            raise NotImplementedError(
                "\n".join(
                    (
                        f"class {cls}'s method '_bind' must be an instance method, ",
                        "and the parameters must be ['self', 'frame'] where frame is a lionqa.Frame used for suppling data source, ",
                        f"and the return type must be None",
                    )
                )
            )

    def __init__(
        self,
        func: Optional[Callable] = None,  # 一个无状态函数，必须是无状态的
        pre: Sequence["Expr"] = tuple(),
    ) -> None:
        self.func = func
        self.pre = tuple(pre)
        self._collected = False
        self._value = None

    def _collect(self) -> Any:
        if not self._collected:
            if self.func is None:
                raise ValueError("数据源尚未定义")
            self._value = self.func(*(pre._collect() for pre in self.pre))
            self._collected = True
            self.func = self.pre = None  # gc
        return self._value

    def _clone(self, clonespace: dict) -> "Expr":
        return Expr(
            self.func,
            tuple(pre.clone(clonespace) for pre in self.pre),
        )

    def _bind(self, frame: "lionqa.Frame"):
        pass

    def collect(self) -> Any:
        expr = self.clone()  # 避免对自己造成修改
        return expr._collect()

    def clone(self, clonespace: Optional[dict] = None) -> "Expr":
        if clonespace is None:
            clonespace = dict()
        if id(self) not in clonespace:
            clonespace[id(self)] = self._clone(clonespace)
        return clonespace[id(self)]

    def bind(self, frame: "lionqa.Frame"):
        """绑定数据源"""
        if len(self.pre) == 0 and self.func is None:  # 如果是根节点并尚未绑定数据源
            self._bind(frame)
        for pre in self.pre:
            pre.bind(frame)
