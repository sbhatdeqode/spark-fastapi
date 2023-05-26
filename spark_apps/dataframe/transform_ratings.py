import os,sys, logging
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)


class TransformRatings():

    def __init__(self, spark_obj):

        self.spark = spark_obj


    def transform(self):

        logger.info("started ratings transforming")

        sc = self.spark.sparkContext

        ratings_rdd = sc.textFile("./data/ratings.dat")

        schemaString = "user_id movie_id rating time_stamp"
        schema = StructType(
            [StructField(field, StringType(), True)\
                for field in schemaString.split(" ")]
        )

        ratings_rdd = ratings_rdd.map(lambda line: line.split("::"))\
            .map(lambda x: Row(x[0], x[1], x[2], x[3]))
        
        ratings_rdd = self.spark.createDataFrame(ratings_rdd, schema)

        ratings_rdd.show(5)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder \
                    .appName("sql_tasks") \
                    .getOrCreate()
obj = TransformRatings(spark)

obj.transform()