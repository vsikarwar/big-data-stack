from mrjob.job import MRJob
from mrjob.job import MRStep

import time
import datetime

class MovieRating(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.map_extract,
                    reducer=self.reducer_collect),
            MRStep(reducer=self.reduce_filter),
            MRStep(reducer=self.reduce_sort)
        ]


    def map_extract(self, _, line):
        splits = line.split('\t')
        if len(splits) == 4:
            (userId, movieId, rating, ts) = line.split('\t')
            yield movieId, (1, rating, '', '')
        else:
            splits = line.split('|')
            #date = time.mktime(datetime.datetime.strptime(splits[2], '%d-%b-%Y').timetuple())
            date = ''
            if splits[2]:
                date = datetime.datetime.strptime(splits[2],'%d-%b-%Y').strftime('%Y%m%d')
            yield splits[0], (0, 0, splits[1], date)

    def reducer_collect(self, key, values):
        count = 0
        movie_rating = 0
        movie_name = ''
        release_date = 0
        for (c, rating, name, date) in values:
            if c == 0 and rating == 0:
                movie_name = name
                release_date = date
            else:
                count += 1
                movie_rating = movie_rating + int(rating)

        avg_rating = movie_rating / count

        yield release_date, (avg_rating, movie_name, release_date)


    def reduce_sort(self, key, values):
        for value in values:
            yield value, key


    def reduce_filter(self, key, values):
        for (avg_rating, movie_name, release_date) in values:
            if avg_rating > 4:
                yield release_date, movie_name


if __name__ == '__main__':
    MovieRating.run()
