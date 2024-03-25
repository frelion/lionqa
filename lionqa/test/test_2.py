from lionqa import QAFrame, Column


class MyData(QAFrame):
    a: int = Column(constraints=[])

    @classmethod
    def read(cls, partition):
        raise NotImplementedError()


def test_qa_frame():
    pass
