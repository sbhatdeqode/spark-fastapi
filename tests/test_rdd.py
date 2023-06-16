"""
    Test module for rdd solutions.
"""

import unittest

from spark_apps.rdd import  \
    most_viewed_movies, distinct_genres, movies_by_genres, \
    movies_starts_with_numbers_letters, latest_movies

from spark_apps.sql import \
       create_database_tables
       

class TestRdd(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        create_obj = create_database_tables.CreateDatabase()
        cls.spark_obj = create_obj.create_database()

    def test_distinct_genres(self):

        expected_result = ["Children's", 'Fantasy', 'Romance', 
                           'Drama', 'Action', 'Thriller', 'Horror',
                           'Sci-Fi', 'Documentary', 'Musical', 
                           'Western', 'Animation', 'Comedy', 'Adventure', 
                           'Crime', 'War', 'Mystery', 'Film-Noir']

        obj = distinct_genres.DistinctGenres(self.spark_obj)
        result = obj.get_distinct_genres()
        self.assertEqual(expected_result, result)

    def test_latest_movies(self):

        latest_movie = "Down to You (2000)"
        obj = latest_movies.LatestMovies(self.spark_obj)
        result = obj.get_latest_movies()
        self.assertIn(latest_movie, result)

    def test_most_viewed_movies(self):

        obj = most_viewed_movies.MostViewedMovies(self.spark_obj)
        result = obj.get_most_viewed_movies()

        expected_result = ('American Beauty (1999)', 3428)
        values = result.values()

        self.assertIn(expected_result, values)
        self.assertEqual(len(values), 10)

    def test_movies_genres(self):

        obj = movies_by_genres.MoviesCountByGenres(self.spark_obj)
        result = obj.get_movies_count()

        expected_result = {
            'Drama': 1603, 'Comedy': 1200, 'Action': 503, 
            'Thriller': 492, 'Romance': 471, 'Horror': 343,
            'Adventure': 283, 'Sci-Fi': 276, "Children's": 251, 
            'Crime': 211, 'War': 143, 'Documentary': 127, 
            'Musical': 114, 'Mystery': 106, 'Animation': 105, 
            'Fantasy': 68, 'Western': 68, 'Film-Noir': 44
        }
        self.assertEqual(expected_result, result)

    def test_movies_starts_with_numbers_letters(self):

        obj = movies_starts_with_numbers_letters.MoviesNumbersLetters(self.spark_obj)
        result = obj.get_movies_count()
        
        self.assertEqual(3848, result['num_of_movies_starts_with_letters'])
        self.assertEqual(30, result['num_of_movies_starts_with_numbers'])



if __name__ == '__main__':
    unittest.main()
                        
                               