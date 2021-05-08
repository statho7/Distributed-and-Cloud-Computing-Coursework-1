import mrjob
from mrjob.job import MRJob


class LinesAppeared(MRJob):

    def mapper(self, key, document):
        for word in document.split(","):
            yield word, document

    def reducer(self, word, document):
        yield word, list(document)


LinesAppeared.run()
