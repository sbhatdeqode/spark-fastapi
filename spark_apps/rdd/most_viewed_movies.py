
import os,sys, logging

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)

class MostViewedMovies():

    def __init__(self, spark_obj):

        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        self.spark = spark_obj
    

    def get_most_viewed_movies(self):
        
        logger.info("started task")

        sc = self.spark.sparkContext


        ratings_rdd = sc.textFile("./data/ratings.dat")

        all_movies = ratings_rdd.map(lambda line: int(line.split("::")[1]))
          
        all_movies = all_movies.map(lambda line: (line, 1))
        all_movies_count = all_movies.reduceByKey(lambda x, y: x + y)
        all_movies_ordered = all_movies_count.takeOrdered(10, lambda x : -x[1])
          
        top10_movies = sc.parallelize(all_movies_ordered)

        movies_rdd = sc.textFile("./data/movies.dat").map(lambda line: (int(line.split("::")[0]), line.split("::")[1]))
        ratings_join_movies = movies_rdd.join(top10_movies)
          
        top_movies_list = ratings_join_movies.takeOrdered(10, lambda x : -x[1][1])
        
        top_movies_list_rdd = sc.parallelize(top_movies_list)

        result_dict = top_movies_list_rdd.collectAsMap()
        
        logger.info("finished task")

        return result_dict
