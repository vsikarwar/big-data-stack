from mrjob.job import MRJob
from mrjob.job import MRStep

class InvertedIndex(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.map_items,
                reducer=self.red_index)
        ]

    def map_items(self, _, line):
        doc_id = line[0]
        words = line[1:].split(' ')
        for word in words:
            yield word , doc_id

    def red_index(self, key, values):
        yield key, list(values)

if __name__ == '__main__':
    InvertedIndex.run()
