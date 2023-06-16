"""
    Test module for sql solutions.
"""

import unittest

from spark_apps.sql import \
       create_database_tables, movies_tasks, ratings_tasks

class TestSql(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        create_obj = create_database_tables.CreateDatabase()
        cls.spark_obj = create_obj.create_database()
        cls.movie_obj = movies_tasks.MovieTasks(cls.spark_obj)
        cls.ratings_obj = ratings_tasks.RatingsTasks(cls.spark_obj)


    def test_oldest_movies(self):

        result = self.movie_obj.get_oldest_movies()
        expected_result = [
            {'movieid': '2821', 'title': 'Male and Female (1919)', 'year': '1919', 'genre': 'Adventure|Drama'}, 
            {'movieid': '2823', 'title': 'Spiders, The (Die Spinnen, 1. Teil: Der Goldene See) (1919)', 'year': '1919', 'genre': 'Action|Drama'}, 
            {'movieid': '3132', 'title': 'Daddy Long Legs (1919)', 'year': '1919', 'genre': 'Comedy'}
            ]

        self.assertEqual(expected_result, result)

    def test_movies_count_by_year(self):

        result = self.movie_obj.get_movies_count_by_year()
        expected_result = {'year': '1953', 'number_of_movies': 14}
        self.assertIn(expected_result, result)

    def test_movies_count_by_ratings(self):

        result = self.ratings_obj.get_movies_count_by_ratings()

        expected_result = [
            {'rating': '1', 'movies_count': 56174}, 
            {'rating': '2', 'movies_count': 107557}, 
            {'rating': '3', 'movies_count': 261197}, 
            {'rating': '4', 'movies_count': 348971}, 
            {'rating': '5', 'movies_count': 226310}
            ]
        
        self.assertEqual(expected_result, result)

    def test_users_count_by_movies(self):

        result = self.ratings_obj.get_users_per_movie()

        expected_result = {'movieid': '3822', 'users_count': 70}

        self.assertIn(expected_result, result)

    def test_total_ratings_per_movie(self):


        result = self.ratings_obj.get_total_ratings_per_movie()

        expected_result = {'movieid': '3940', 'total_ratings': 26.0}

        self.assertIn(expected_result, result)

    
    def test_avg_ratings_per_movie(self):

        result = self.ratings_obj.get_avg_ratings_per_movie()
        expected_result = {'movieid': '3942', 'total_ratings': 1.7857142857142858}
        self.assertIn(expected_result, result)


if __name__ == '__main__':
    unittest.main()


    

