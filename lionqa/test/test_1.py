from lionqa import Frame, Column


import pandas as pd

def test_frame():
    df = pd.DataFrame({
        "a": range(10),
        "b": range(5, 15)
    })
    frame = Frame(df)
    print(frame.collect())
    print(frame.where(Column("a") > 1).collect())
    print(frame.where((Column("a") > 1)&(Column("b")<9)).collect())

def test_column():
    df = pd.DataFrame({
        "a": range(10),
        "b": range(5, 15)
    })
    frame = Frame(df)
    print(frame.collect())
    print(frame["a"].collect())


