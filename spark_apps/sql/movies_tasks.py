from pyspark.sql import SparkSession
import os,sys, logging
from .create_database_tables import CreateDatabase

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)

class MovieTasks():

    def __init__(self, spark_obj):

        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        self.spark = spark_obj
        
    def get_oldest_movies(self):

        logger.info("computing oldest movies")

        query = """
                    SELECT *
                    FROM movies
                    WHERE year =(
                    SELECT min(year)
                    FROM moviebase.movies
                    )
                """

        old_movies = self.spark.sql(query)
        list_movies = list(map(lambda row: row.asDict(), old_movies.collect()))
        
        logger.info("computing finished")
        
        return list_movies
    
    def get_movies_count_by_year(self):

        logger.info("computing movies count by year")

        
        query = """
                    SELECT year, count(year) as number_of_movies
                    FROM moviebase.movies
                    group by year
                    
                """
        movies_list = self.spark.sql(query)
        movies_list = list(map(lambda row: row.asDict(), movies_list.collect()))
        
        logger.info("computing finished")
        return movies_list
        
