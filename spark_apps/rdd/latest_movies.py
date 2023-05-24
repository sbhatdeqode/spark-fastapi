from pyspark.sql import SparkSession
import os,sys, logging

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)

class LatestMovies():

    def __init__(self):

        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        self.spark = SparkSession.builder \
                    .appName("rdd_tasks") \
                    .getOrCreate()
    

    def get_latest_movies(self):

        logger.info("started task")

        sc = self.spark.sparkContext

        movies_rdd = sc.textFile("./data/movies.dat")

        year = movies_rdd.map(lambda lines: lines.split("(")[-1].split(")")[0])
        latest = year.max()

        latest_movies = movies_rdd.filter(lambda lines: "("+latest+")" in lines)

        logger.info("finished task")

        return latest_movies.map(lambda lines: lines.split("::")[1]).collect()

if __name__ == '__main__':

    obj = LatestMovies()
    print(obj.get_latest_movies()) 