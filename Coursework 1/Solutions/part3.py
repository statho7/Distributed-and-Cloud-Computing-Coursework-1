import mrjob
from mrjob.job import MRJob


class WordSequence(MRJob):

    def mapper(self, key, document):
        for line in document.split("\n"):
            words = line.split(" ")
            if len(words) > 4:
                for i in range(0, len(words) - 3):
                    sequence = words
                    phrase = sequence[i:i+4]
                    yield ' '.join(phrase), 1
            elif len(words) == 4:
                yield line, 1

    def reducer(self, phrase, occurrences):
        yield phrase, sum(occurrences)


WordSequence.run()
