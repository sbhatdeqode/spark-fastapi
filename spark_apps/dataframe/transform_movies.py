"""
    Module to transform movies data.
"""

import os,sys, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, substring

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)


class TransformMovies():

    """
        Class to transform movies data.
    """

    def __init__(self, spark_obj):

        self.spark = spark_obj


    def transform(self):

        """
            method to transform movies data.
        """

        logger.info("started movie transforming")

        movie_df = self.spark.read.text("./data/movies.dat")


        movie_df = movie_df.select(
            split(movie_df.value, "::")[0].alias("id"),
             split(movie_df.value, "::")[1].alias("movie_title"),
            split(movie_df.value, "::")[2].alias("genre")

        )
        split_col = split(movie_df["movie_title"], "\\(")
        movie_df = movie_df.withColumn("title", split_col.getItem(0))
        movie_df = movie_df.withColumn("year_n", split_col.getItem(1))
        movie_df = movie_df.withColumn("year", substring(movie_df["year_n"], 0, 4))

        movie_df = movie_df.drop(*["movie_title","year_n"])
        new_column_order = ["id", "title", "year", "genre"]
        movie_df = movie_df.select(new_column_order)
        
        movie_df.show(5)
        
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder \
                    .appName("df_tasks") \
                    .getOrCreate()
obj = TransformMovies(spark)

obj.transform()

        
              
        




