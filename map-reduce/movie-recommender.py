# using MR job framework
from mrjob.job import MRJob
from mrjob.job import MRStep
from math import sqrt

from itertools import combinations

class MovieRecommender(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.parse_input,
                    reducer=self.reduce_ratings_by_user),
            MRStep(mapper=self.mapper_item_pairs,
                    reducer=self.reducer_similarity),
            MRStep(mapper=self.mapper_sort_sim,
                    mapper_init=self.load_movie_names,
                    reducer=self.reducer_output_sim)
        ]

    def parse_input(self, key, line):
        (userId, movieId, rating, ts) = line.split('\t')
        yield userId, (movieId, float(rating))

    def reduce_ratings_by_user(self, key, values):
        ratings = []

        for movieId, rating in values:
            ratings.append((movieId, rating))

        yield key, ratings

    def mapper_item_pairs(self, key, value):
        for item1, item2 in combinations(value, 2):
            yield (item1[0], item2[0]), (item1[1], item2[1])
            yield (item2[0], item1[0]), (item2[1], item1[1])


    def reducer_similarity(self, key, value):
        score, numPairs = self.cosine_similarity(value)

        if numPairs > 10 and score > 0.95:
            yield key, (score, numPairs)

    def cosine_similarity(self, ratingPair):
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0

        for ratingX, ratingY in ratingPair:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if(denominator):
            score = (numerator / float(denominator))

        return (score, numPairs)

    def mapper_sort_sim(self, key, value):
        score, n = value
        movie1, movie2 = key

        yield (self.movieNames[int(movie1)], score), (self.movieNames[int(movie2)], n)


    def reducer_output_sim(self, key, value):
        movie1, sim = key
        for movie2, n in value:
            yield movie1, (movie2, score, n)

    def load_movie_names(self):
        self.movieNames = {}

        with open("u.item") as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[int(fields[0])] = fields[1].decode('utf-8', 'ignore')


if __name__ == '__main__':
    MovieRecommender.run()
