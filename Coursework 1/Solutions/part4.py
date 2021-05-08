import mrjob
from mrjob.job import MRJob


class URLlink(MRJob):

    def mapper(self, key, document):
        comb = document.split(",")
        yield comb[0], document
        yield comb[1], document

    def reducer(self, word, document):
        my_list = list(document)
        for comb in my_list:
            temp = [x for x in my_list if x != comb]
            for link in temp:
                if comb[-4:] == link[:4]:
                    yield "_", comb + f",{link[5:]}"


URLlink.run()
