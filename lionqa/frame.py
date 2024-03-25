import datetime as dt
from abc import ABC, abstractmethod
from typing import Literal, Union, Any

import pandas as pd

import lionqa
from lionqa.expr import Expr, make
from lionqa.constraint import Constraint


class Frame(Expr):
    """数据帧的抽象"""

    def __init__(self, *args, **kwargs) -> None:
        if len(args) == 1 and isinstance(args[0], pd.DataFrame):
            super().__init__(lambda: args[0])
        else:
            super().__init__(*args, **kwargs)

    def clone(self, clonespace: dict) -> "Frame":
        if id(self) not in clonespace:
            clonespace[id(self)] = Frame(
                func=self.func, pre=tuple(pre.clone(clonespace) for pre in self.pre)
            )
        return clonespace[id(self)]

    def __getitem__(self, column_name: str) -> Expr:
        """其实是返回一个Column"""
        return Expr(func=lambda df: df[column_name], pre=(self,))

    def select(self, *args: "lionqa.Column") -> "Frame":
        cols = [f"{col.frame_name}.{col.column_name}" for col in args]
        return Frame(func=lambda df, cols: df[cols], pre=(self, make(cols)))

    def where(self, expr: Expr) -> "Frame":
        expr = expr.clone(dict())
        for root in expr.roots():
            if not isinstance(root, lionqa.Column):
                continue
            root.set(self)
        return Frame(func=lambda df, mask: df[mask], pre=(self, expr))

    def merge(
        self, frame: "Frame", on: Expr, how: Literal["left", "right", "inner"]
    ) -> "Frame":
        return Frame(
            func=lambda df1, df2, on, how: df1.merge(df2, on=on, how=how),
            pre=(self, frame, on, make(how)),
        )


class QAFrameMeta(type(ABC), type):
    """绑定检测逻辑和待检测数据"""

    __frame_set__ = set()
    __bussiness_attrabutes__ = [
        "__name__",
        "__constraints__",
        "__cache__",
        "read",
        "__partition__",
        "__default_partition__",
    ]

    def __new__(cls, name, bases, namespace):
        if "read" not in namespace:
            raise ValueError(f"Please implement {name} class methods 'read'")
        elif not isinstance(namespace["read"], classmethod):
            raise TypeError(f"class {name} 'read' attribute is not classmethod type")
        namespace["__annotations__"] = namespace.get("__annotations__", dict())
        namespace["__name__"] = namespace.get("__name__", name)
        namespace["__constraints__"] = namespace.get("__constraints__", list())
        namespace["__cache__"] = dict()
        for field, value in list(namespace.items()):
            if field in namespace["__annotations__"]:
                if not isinstance(value, lionqa.Column):
                    raise TypeError(
                        "Type annotations can only be used for column definitions"
                    )
            elif field == "__name__":
                if not isinstance(value, str):
                    raise TypeError("__name__ must be str")
                elif value in cls.__frame_set__:
                    raise ValueError(f"{value} has been defined")
            elif field == "__constraints__":
                if not isinstance(value, list):
                    raise TypeError("__constraints__ must be list")
                for v in value:
                    if not isinstance(v, Constraint):
                        raise TypeError(
                            "Elements in __constraints__ must be of type Constraint"
                        )
            elif isinstance(value, lionqa.Column):
                if field not in namespace["__annotations__"]:
                    namespace["__annotations__"][field] = Any
        _cls = super().__new__(cls, name, bases, namespace)
        cls.__frame_set__.add(_cls.__name__)
        return _cls


class QAFrame(Frame, ABC, metaclass=QAFrameMeta):
    """用户自定义待检测数据帧接口"""

    @classmethod
    @abstractmethod
    def read(cls, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError()

    @classmethod
    def __read__(cls, partition: Any) -> pd.DataFrame:
        if partition not in cls.__cache__:
            return cls.read(partition)
        return cls.__cache__[partition]

    def __init__(self, partition: Union[dt.date, str]) -> None:
        self.partition = partition
        super().__init__(func=self.__class__.__read__, pre=(make(self.partition),))
