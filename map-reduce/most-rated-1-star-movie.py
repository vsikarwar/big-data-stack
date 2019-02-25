# using MR job framework
from mrjob.job import MRJob
from mrjob.job import MRStep

class SimpleCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_item,
                    reducer=self.reducer_reduce),
            MRStep(reducer=self.reducer_sort)
        ]

    def mapper_get_item(self, _, line):
        splits = line.split('\t')
        if len(splits) == 4:
            (userId, movieId, rating, ts) = line.split('\t')
            yield movieId, (1, rating, '')
        else:
            splits = line.split('|')
            #date = time.mktime(datetime.datetime.strptime(splits[2], '%d-%b-%Y').timetuple())
            yield splits[0], (0, 0, splits[1])


    def reducer_reduce(self, key, values):
        count = 0
        rating = 0
        name = ''
        for (c, r, n) in values:
            if c==0 and r==0:
                name = n
            else:
                if r == '1':
                    count += 1
                    rating = int(r)
        if rating == 1:
            yield None, (count, rating, name)

    def reducer_sort(self, key, values):
        for (count, rating, name) in sorted(values):
            yield name, (rating, count)


if __name__ == '__main__':
    SimpleCount.run()
