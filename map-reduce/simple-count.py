# using MR job framework
from mrjob.job import MRJob
from mrjob.job import MRStep

class SimpleCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_item,
                    reducer=self.reducer_count_item)
        ]

    def mapper_get_item(self, _ , line):
        (_, _, rating, ts) = line.split('\t')
        yield rating, 1

    def reducer_count_item(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    SimpleCount.run()
