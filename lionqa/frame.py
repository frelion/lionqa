import datetime as dt
from types import SimpleNamespace
from abc import ABC, abstractmethod
from typing import Literal, Any, Optional, Union, Tuple, Optional

import pandas as pd
from multimethod import multimethod

import lionqa
from lionqa.expr import Expr, make
from lionqa.column import Column
from lionqa.constraint import Constraint


class Frame(Expr):
    """数据帧的抽象"""

    @multimethod
    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__(func=lambda df: df, pre=(make(df),))
        self.__columns__ = [Column(col) for col in df.columns]
        self.__set_columns__()
    
    @multimethod
    def __init__(self, func, pre=tuple(), columns=None):
        super().__init__(pre=pre, func=func)
        self.__columns__ = columns or list()
        self.__set_columns__()

    def __set_columns__(self):
        _column_name_count = dict()
        for col in self.__columns__:
            col.bind(self)
            _column_name_count[col.column_name] = (
                _column_name_count.get(col.column_name, 0) + 1
            )
        for col in self.__columns__:
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
    ]

    @staticmethod
    def _foramt(name, namespace):
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
    # TODO: 加入constraints检查

    @classmethod
    @abstractmethod
    def read(cls, index: Optional[Union[dt.date, dt.datetime, str]]) -> pd.DataFrame:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def read_offset(cls, anchor: Optional[Union[dt.date, dt.datetime, str]], offset: int) -> pd.DataFrame:
        raise NotImplementedError()

    def __read__(self):
        if self.is_offset:
            return self.__class__.read_offset(self.anchor, self.index_or_offset)
        else:
            return self.__class__.read(self.index_or_offset)

    def __init__(self, index_or_offset: Optional[Union[int, dt.datetime, dt.date, str]] = None) -> None:
        self.is_offset = isinstance(index_or_offset, int)
        self.index_or_offset = index_or_offset
        self.anchor = None
        columns = list()
        for col in self.__class__.__columns__:
            col = getattr(self.__class__, col).clone()
            col.unbind()
            columns.append(col)
        super().__init__(
            func=self.__read__,
            columns=columns,
        )

    def check(self):
        for cons in self.__class__.__constraints__:
            assert cons(self).collect()
        for column in self.__columns__:
            for cons in column.constraints:
                assert cons(self).check()
