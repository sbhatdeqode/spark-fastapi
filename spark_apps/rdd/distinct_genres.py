from pyspark.sql import SparkSession
import logging, os, sys

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)

class DistinctGenres():

    def __init__(self, spark_obj):
        
        self.spark = spark_obj
        
        
    def get_distinct_genres(self):

        logger.info("started task")

        sc = self.spark.sparkContext

        movies_rdd = sc.textFile("./data/movies.dat")

        movies_rdd = movies_rdd.map(lambda line: line.split("::")[2])

        movies_rdd = movies_rdd.flatMap(lambda line: line.split("|"))

        logger.info("finished task")

        return movies_rdd.distinct().collect()
