from mrjob.job import MRJob
from mrjob.job import MRStep

class SortMovieCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.map_movieId,
                    reducer=self.red_movie_count),
            MRStep(reducer=self.red_sort)
        ]

    def map_movieId(self, _, line):
        (userId, movieId, rating, ts) = line.split('\t')
        yield movieId, 1

    def red_movie_count(self, key, values):
        # since the values are int string
        yield str(sum(values)).zfill(5), key

    def red_sort(self, count, movies):
        yield count, list(movies)

if __name__ == '__main__':
    SortMovieCount.run()
