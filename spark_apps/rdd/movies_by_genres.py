from pyspark.sql import SparkSession
import os,sys, logging

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)

class MoviesCountByGenres():

    def __init__(self, spark_obj):

        self.spark = spark_obj
        
    def get_movies_count(self):

        logger.info("started task")

        sc = self.spark.sparkContext

        movies_rdd = sc.textFile("./data/movies.dat")

        movies_rdd = movies_rdd.map(lambda line: line.split("::")[2]) \
                    .flatMap(lambda line: line.split("|")) \
                    .map(lambda x: (x,1)) \
                    .reduceByKey(lambda x,y: x+y) \
                    .sortBy(lambda x: x[1], ascending=False)
        
        logger.info("finished task")
        
        return movies_rdd.collectAsMap()
    

        