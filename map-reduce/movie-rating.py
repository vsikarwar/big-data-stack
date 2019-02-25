from mrjob.job import MRJob
from mrjob.job import MRStep

class MovieRating(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_movie_id,
                    reducer=self.reducer_count)
        ]

    def mapper_movie_id(self, _, line):
        (userId, movieId, rating, ts) = line.split('\t')
        yield movieId, 1

    def reducer_count(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MovieRating.run()
