import os,sys, logging
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import split, substring
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)


class TransformUsers():

    def __init__(self, spark_obj):

        self.spark = spark_obj


    def transform(self):

        logger.info("started users transforming")

        sc = self.spark.sparkContext

        user_rdd = sc.textFile("./data/users.dat")

        schemaString = "UserID Gender Age Occupation Zip-code"
        schema = StructType(
            [StructField(field, StringType(), True)\
                for field in schemaString.split(" ")]
        )

        user_rdd = user_rdd.map(lambda line: line.split("::"))\
            .map(lambda x: Row(x[0], x[1], x[2], x[3], x[4]))
        
        user_rdd = self.spark.createDataFrame(user_rdd, schema)

        user_rdd.show(5)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder \
                    .appName("sql_tasks") \
                    .getOrCreate()
obj = TransformUsers(spark)

obj.transform()
