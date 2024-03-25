from lionqa import QAFrame, Column


import pandas as pd


df = pd.DataFrame({"a": range(100, 110), "b": range(10)})


class MyData2(QAFrame):
    a: int = Column(constraints=[])
    b: int = Column()

    @classmethod
    def read(cls, partition):
        return df


def test_qa_frame():
    print()
    # print(MyData2().where(MyData2.a < 105).collect())
    # print(MyData2().where(MyData2.a < 105).where(MyData2.a > 101).collect())
    # print(MyData2().where(MyData2.a < 105).a.collect())
    print(MyData2().where(MyData2.a < 105).collect())
