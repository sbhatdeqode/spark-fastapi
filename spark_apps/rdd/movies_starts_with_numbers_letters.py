"""
    Module to get Movies starts with Numbers or Letters.
"""

import logging

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)


class MoviesNumbersLetters():

    """
        class to get Movies starts with Numbers or Letters.
    """

    def __init__(self, spark_obj):

        self.spark = spark_obj


    def get_movies_count(self):

        """
            method to get Movies starts with Numbers or Letters.
        """

        logger.info("started task")

        sc = self.spark.sparkContext

        movies_rdd = sc.textFile("./data/movies.dat")

        movies_rdd = movies_rdd.map(lambda line: line.split("::")[1])\
                        .map(lambda line:line.split(" ")[0])
        
        movies_rdd_alpha = movies_rdd.filter(lambda word: word[0].isalpha())\
        
        alpha_count = movies_rdd_alpha.count()
        
        movies_rdd_num = movies_rdd.filter(lambda word: word[0].isdigit())\
        
        num_count = movies_rdd_num.count()

        logger.info("finished task")

        return {
            "num_of_movies_starts_with_letters": alpha_count,
            "num_of_movies_starts_with_numbers": num_count,
            "total_movies": alpha_count+num_count
        }
