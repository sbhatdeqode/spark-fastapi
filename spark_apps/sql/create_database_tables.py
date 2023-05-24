from pyspark.sql import SparkSession
import os,sys, logging

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)

class CreateDatabase():

    def __init__(self):

        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        self.spark = SparkSession.builder \
                    .appName("sql_tasks") \
                    .getOrCreate()
        
    def create_database(self):

        logger.info("started creating a database and tables")

        self.spark.sql("DROP DATABASE IF EXISTS moviebase CASCADE")
        self.spark.sql("CREATE DATABASE moviebase")
        self.spark.sql("USE moviebase")

        logger.info("database created")  

        movies_data = self.spark.read.text("./data/movies.dat")
        movies_data.createOrReplaceTempView("movies_temp")

        ratings_data = self.spark.read.text("./data/ratings.dat")
        ratings_data.createOrReplaceTempView("ratings_temp")

        users_data = self.spark.read.text("./data/users.dat")
        users_data.createOrReplaceTempView("users_temp")
        
        self.spark.sql(
            """
            SELECT
                split(value, '::')[0] AS movieid,
                split(value, '::')[1] AS title,
                substring(split(value, '::')[1], length(split(value, '::')[1]) - 4, 4) AS year,
                split(value, '::')[2] AS genre
            FROM movies_temp
            """
        ).write.mode("overwrite").saveAsTable("moviebase.movies")

        logger.info("movies table created")

       
        self.spark.sql(
            """
            
            SELECT
                split(value, '::')[0] AS userid,
                split(value, '::')[1] AS gender,
                split(value, '::')[2] AS age,
                split(value, '::')[3] AS occupation,
                split(value, '::')[4] AS zipcode
            FROM users_temp
            """
            ).write.mode("overwrite").saveAsTable("moviebase.users")
        
        logger.info("users table created")

        
        self.spark.sql(
            """
            SELECT
                split(value, '::')[0] AS userid,
                split(value, '::')[1] AS movieid,
                split(value, '::')[2] AS rating,
                split(value, '::')[3] AS timestamp
            FROM ratings_temp
        """).write.mode("overwrite").saveAsTable("moviebase.ratings")

        logger.info("ratings table created")

        

if __name__ == "__main__":

    obj = CreateDatabase()

    obj.create_database()