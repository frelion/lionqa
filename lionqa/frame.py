from types import SimpleNamespace
from abc import ABC, abstractmethod
from typing import Literal, Any, Optional, Union, Tuple, List

import pandas as pd

import lionqa
from lionqa.expr import Expr, make
from lionqa.column import Column
from lionqa.constraint import Constraint
from lionqa.partition import Partition, DefaultPartition


class Frame(Expr):
    """数据帧的抽象"""

    def __init__(self, *args, columns: Optional[List[Column]] = None, **kwargs) -> None:
        if len(args) == 1 and isinstance(args[0], pd.DataFrame) and len(kwargs) == 0:
            super().__init__(func=lambda df: df, pre=(make(args[0]),))
            columns = [Column(col) for col in args[0].columns]
        else:
            super().__init__(*args, **kwargs)
            columns = columns or list()
        _column_name_count = dict()
        for col in columns:
            col.bind(self)
            _column_name_count[col.column_name] = (
                _column_name_count.get(col.column_name, 0) + 1
            )
        for col in columns:
            if col.frame_name is None:
                if hasattr(self, col.column_name):
                    raise ValueError(f"{col.column_name} duplicate column")
                setattr(self, col.column_name, col)
            else:
                if not hasattr(self, col.frame_name):
                    setattr(self, col.frame_name, SimpleNamespace())
                if hasattr(getattr(self, col.frame_name), col.column_name):
                    raise ValueError(
                        f"{col.frame_name}.{col.column_name} duplicate column"
                    )
                setattr(getattr(self, col.frame_name), col.column_name, col)
                if _column_name_count[col.column_name] == 1:
                    setattr(self, col.column_name, col)
        self.__columns__ = columns

    def _clone(self, clonespace: dict) -> "Frame":
        return Frame(
            func=self.func, pre=tuple(pre.clone(clonespace) for pre in self.pre)
        )

    def _bind(self, frame: "Frame"):
        pass

    def __getitem__(self, index: Union[Tuple[Optional[str]], str]) -> Expr:
        if isinstance(index, str):
            return Expr(func=lambda df: df[index], pre=(self,))
        elif isinstance(index, tuple):
            frame_name, column_name = index

            def inner(df: pd.DataFrame):
                if (
                    frame_name is not None
                    and f"{frame_name}.{column_name}" in df.columns
                ):
                    return df[f"{frame_name}.{column_name}"]
                else:
                    return df[column_name]

            return Expr(func=inner, pre=(self,))

    def select(self, *args: "lionqa.Column") -> "Frame":
        cols = [f"{col.frame_name}.{col.column_name}" for col in args]
        columns = list()
        for col in self.__columns__:
            col = col.clone()
            col.unbind()
            columns.append(col)
        return Frame(
            func=lambda df, cols: df[cols], pre=(self, make(cols)), columns=columns
        )

    def where(self, expr: Expr) -> "Frame":
        expr = expr.clone()  # 避免修改原表达式
        expr.bind(self)  # 将自己的数据绑定在该表达式上
        columns = list()
        for col in self.__columns__:
            col = col.clone()
            col.unbind()
            columns.append(col)
        return Frame(func=lambda df, mask: df[mask], pre=(self, expr), columns=columns)

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
        "__partition__",
    ]

    @staticmethod
    def _foramt(name, namespace):
        if "read" not in namespace:
            raise ValueError(f"Please implement {name} class methods 'read'")
        elif not isinstance(namespace["read"], classmethod):
            raise TypeError(f"class {name} 'read' attribute is not classmethod type")
        if name in QAFrameMeta.__frame_set__:
            raise SyntaxError(f"class {name} duplicate definition")
        if "__partition__" not in namespace:
            namespace["__partition__"] = DefaultPartition()
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
            elif field == "__partition__":
                if not isinstance(value, Partition):
                    raise TypeError(f"__partition__ field must be of type Partition")
            if field in __annotations__:
                if not isinstance(value, lionqa.Column):
                    raise TypeError(
                        "Type annotations can only be used for column definitions"
                    )
            elif isinstance(value, lionqa.Column):
                if field not in __annotations__:
                    __annotations__[field] = Any
        namespace["__columns__"] = __annotations__

    def __new__(cls, name, bases, namespace):
        QAFrameMeta._foramt(name, namespace)
        for column_name in namespace["__columns__"]:
            column = namespace[column_name]
            column.frame_name = name
            column.column_name = column_name
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

    def __init__(self, index: Optional[Any] = None) -> None:
        self.partition, self.is_offset = self.__class__.__partition__[index]
        if not self.is_offset:
            columns = list()
            for col in self.__class__.__columns__:
                col = getattr(self.__class__, col).clone()
                col.unbind()
                columns.append(col)
            super().__init__(
                func=self.__class__.__read__,
                pre=(make(self.partition),),
                columns=columns,
            )
        else:
            super().__init__()
