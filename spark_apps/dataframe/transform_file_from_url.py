import os,sys, logging
from pyspark import SparkFiles
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format=' %(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)


class TransformImport():

    def __init__(self, spark_obj):

        self.spark = spark_obj
        gender_count = self.spark.sparkContext.longAccumulator("Accumulator")


    def transform(self):

        logger.info("started importing file and transforming")

        url = "https://raw.githubusercontent.com/Thomas-George-T/Movies-Analytics-in-Spark-and-Scala/master/Movielens/users.dat"

        self.spark.sparkContext.addFile(url)

    
        df = spark.read.option("sep", "::")\
                        .csv("file://" + SparkFiles.get("users.dat"))
        df = df.selectExpr("_c0 as user_id", "_c1 as gender", "_c2 as age", "_c3 as occupation", "_c4 as Zip_code")
       
        return df

    
    def broadcast(self):

        df = self.transform()
        broadcast_var = spark.sparkContext.broadcast(df.collect())

        
        print(broadcast_var.value)

    def accumulator(self):

        df = self.transform()
        df.foreach(self.count_gender)
        print(self.gender_count.value)


    def count_gender(self, line):

        
        if "M" in line:
            self.gender_count.add(1)



os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder \
                    .appName("sql_tasks") \
                    .getOrCreate()
obj = TransformImport(spark)

obj.accumulator()