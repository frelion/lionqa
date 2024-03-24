import lionqa


class MyData(lionqa.QAFrame):
    a: int = lionqa.Column()

    @classmethod
    def read(cls, partition):
        raise NotImplementedError()


def test_qa_frame():
    pass
