import mrjob
from mrjob.job import MRJob


class FindMax(MRJob):

    def mapper(self, key, document):
        for char in document.split(","):
            yield "_", float(char)

    def reducer(self, char, numbers):
        yield char, max(numbers)


FindMax.run()
