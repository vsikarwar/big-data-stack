from mrjob.job import MRJob
from mrjob.job import MRStep

class MovieRating(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_movie_id,
                    reducer=self.reducer_count)
        ]

    def mapper_movie_id(self, _, line):
        splits = line.split('\t')
        if len(splits) == 4:
            (userId, movieId, rating, ts) = line.split('\t')
            yield movieId, (1, '')
        else:
            splits = line.split('|')
            yield splits[0], (0, splits[1])


    def reducer_count(self, key, values):
        n = None
        count = 0
        for (c, name) in values:
            if c == 0:
                n = name
            else:
                count += 1
        yield n, count


if __name__ == '__main__':
    MovieRating.run()
