from pyspark.sql import SparkSession
import os
import sys
import logging


logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)

def create_session():
  spk = SparkSession.builder \
      .appName("rdd_tasks") \
      .getOrCreate()
  return spk

def top_movies():

  os.environ['PYSPARK_PYTHON'] = sys.executable
  os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

  spark = create_session()
  sc = spark.sparkContext

  movies_rdd = sc.textFile("./data/movies.dat")

  movies_rdd = movies_rdd.map(lambda line: line.split("::")[2]) \
                    .flatMap(lambda line: line.split("|")) \
                    .map(lambda x: (x,1)) \
                    .reduceByKey(lambda x,y: x+y)
        
  print(movies_rdd.take(5))

  return movies_rdd.collectAsMap()

   

if __name__ == "__main__":

  top_movies()
  

    
    


    

    

   

    
    
   