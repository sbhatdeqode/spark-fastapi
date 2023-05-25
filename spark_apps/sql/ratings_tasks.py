import os,sys, logging
from .create_database_tables import CreateDatabase

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)

class RatingsTasks():

    def __init__(self, spark_obj):

        self.spark = spark_obj
        self.spark.sql("use moviebase")


    def get_movies_count_by_ratings(self):

        logger.info("computing movies_by_ratings")
        movies_count = self.spark.sql(
            """
            SELECT rating, count(rating) as movies_count
            FROM ratings 
            group by rating
            order by rating asc
            """
        )

        list_movies = list(map(lambda row: row.asDict(), movies_count.collect()))
    
        logger.info("computing finished")
        return list_movies
    

    def get_users_per_movie(self):
    
        logger.info("computing users per movie")

        users_count = self.spark.sql(
            """
            SELECT movieid,
            count(userid) as users_count
            FROM ratings 
            group by movieid
            order by cast(movieid as int) asc
           
            """
        )

        list_users = list(map(lambda row: row.asDict(), users_count.collect()))
    
        logger.info("computing finished")
        return list_users
        


    def get_total_ratings_per_movie(self):
    
        logger.info("computing total_ratings_per_movie")

        ratings_count = self.spark.sql(
            """
            SELECT movieid,
            sum(rating) as total_ratings
            FROM ratings 
            group by movieid
            order by cast(movieid as int) asc
           
            """
        )

        list_ratings_count = list(map(lambda row: row.asDict(), ratings_count.collect()))
    
        logger.info("computing finished")
        return list_ratings_count
    

    def get_avg_ratings_per_movie(self):
    
        logger.info("computing avg_ratings_per_movie")

        ratings_avg = self.spark.sql(
            """
            SELECT movieid,
            avg(rating) as total_ratings
            FROM ratings 
            group by movieid
            order by cast(movieid as int) asc
           
            """
        )

        list_ratings_avg = list(map(lambda row: row.asDict(), ratings_avg.collect()))
    
        logger.info("computing finished")
        return list_ratings_avg

