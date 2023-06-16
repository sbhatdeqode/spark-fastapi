"""
    Module to get Latest Movies.
"""

import logging

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)


class LatestMovies():

    """
        class to get Latest Movies.
    """

    def __init__(self, spark_obj):

        self.spark = spark_obj
    

    def get_latest_movies(self):

        """
            method to get Latest Movies.
        """

        logger.info("started task")

        sc = self.spark.sparkContext

        movies_rdd = sc.textFile("./data/movies.dat")

        year = movies_rdd.map(lambda lines: lines.split("(")[-1].split(")")[0])
        latest = year.max()

        latest_movies = movies_rdd.filter(lambda lines: "("+latest+")" in lines)

        logger.info("finished task")

        return latest_movies.map(lambda lines: lines.split("::")[1]).collect()
