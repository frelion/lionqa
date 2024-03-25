from abc import ABC, abstractmethod
from typing import Literal, Any, Optional, List

import pandas as pd

import lionqa
from lionqa.expr import Expr, make
from lionqa.column import Column
from lionqa.constraint import Constraint


class Frame(Expr):
    """数据帧的抽象"""

    def __init__(self, *args, **kwargs) -> None:
        if len(args) == 1 and isinstance(args[0], pd.DataFrame) and len(kwargs) == 0:
            super().__init__(func=lambda df: df, pre=(make(args[0]),))
        else:
            super().__init__(*args, **kwargs)

    def _clone(self, clonespace: dict) -> "Frame":
        return Frame(
            func=self.func, pre=tuple(pre.clone(clonespace) for pre in self.pre)
        )

    def _bind(self, frame: "Frame"):
        pass

    def __getitem__(self, column_name: str) -> Expr:
        return Expr(func=lambda df: df[column_name], pre=(self,))

    def select(self, *args: "lionqa.Column") -> "Frame":
        cols = [f"{col.frame_name}.{col.column_name}" for col in args]
        return Frame(func=lambda df, cols: df[cols], pre=(self, make(cols)))

    def where(self, expr: Expr) -> "Frame":
        expr = expr.clone()  # 避免修改原表达式
        expr.bind(self)  # 将自己的数据绑定在该表达式上
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
        "__constraints__",
        "__columns__",
        "__cache__",
        "read",
        "__partition_by__",
        "__default_partition__",
    ]

    @staticmethod
    def _foramt(name, namespace):
        if "read" not in namespace:
            raise ValueError(f"Please implement {name} class methods 'read'")
        elif not isinstance(namespace["read"], classmethod):
            raise TypeError(f"class {name} 'read' attribute is not classmethod type")
        if name in QAFrameMeta.__frame_set__:
            raise SyntaxError(f"class {name} duplicate definition")
        __annotations__ = namespace.get("__annotations__", dict())
        namespace["__constraints__"] = namespace.get("__constraints__", list())
        namespace["__cache__"] = dict()
        for field, value in list(namespace.items()):
            if field == "__constraints__":
                if not isinstance(value, list):
                    raise TypeError("__constraints__ must be list")
                for v in value:
                    if not isinstance(v, Constraint):
                        raise TypeError(
                            "Elements in __constraints__ must be of type Constraint"
                        )
            elif field == "__columns__":
                raise ValueError(
                    "field '__columns__' is to retain key fields, users are not defined"
                )
            elif field in __annotations__:
                if not isinstance(value, lionqa.Column):
                    raise TypeError(
                        "Type annotations can only be used for column definitions"
                    )
            elif isinstance(value, lionqa.Column):
                if field not in __annotations__:
                    __annotations__[field] = Any
        namespace["__columns__"] = __annotations__

    @staticmethod
    def _bind_column_constraints(columns: List[Column]): ...

    def __new__(cls, name, bases, namespace):
        QAFrameMeta._foramt(name, namespace)
        _cls = super().__new__(cls, name, bases, namespace)
        QAFrameMeta.__frame_set__.add(name)
        return _cls


class QAFrame(Frame, ABC, metaclass=QAFrameMeta):
    """用户自定义待检测数据帧接口"""

    @classmethod
    @abstractmethod
    def read(cls, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError()

    @classmethod
    def __read__(cls, partition: Optional[Any] = None) -> pd.DataFrame:
        if partition not in cls.__cache__:
            return cls.read(partition)
        return cls.__cache__[partition]

    def __init__(self, partition_or_offset: Optional[Any] = None) -> None:
        self.partition_or_offset = partition_or_offset
        if self.partition_or_offset in self.__class__.__partition_by__:
            super().__init__(
                func=self.__class__.__read__, pre=(make(self.partition_or_offset),)
            )
        else:
            super().__init__()

        for column in self.__class__.__columns__:
            setattr(self, column, getattr(self.__class__, column).clone({}))
