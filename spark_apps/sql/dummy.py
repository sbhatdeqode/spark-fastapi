from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Read movies file and create temporary view
movies_staging = spark.read.textFile("../../Movielens/movies.dat")
movies_staging.createOrReplaceTempView("movies_staging")

# Read ratings file and create temporary view
ratings_staging = spark.read.textFile("../../Movielens/ratings.dat")
ratings_staging.createOrReplaceTempView("ratings_staging")

# Read users file and create temporary view
users_staging = spark.read.textFile("../../Movielens/users.dat")
users_staging.createOrReplaceTempView("users_staging")

# Create a database to store the tables
spark.sql("DROP DATABASE IF EXISTS sparkdatalake CASCADE")
spark.sql("CREATE DATABASE sparkdatalake")

# Make appropriate schemas for the tables
# movies
spark.sql("""
    SELECT
        split(value, '::')[0] AS movieid,
        split(value, '::')[1] AS title,
        substring(split(value, '::')[1], length(split(value, '::')[1]) - 4, 4) AS year,
        split(value, '::')[2] AS genre
    FROM movies_staging
""").write.mode("overwrite").saveAsTable("sparkdatalake.movies")

# users
spark.sql("""
    SELECT
        split(value, '::')[0] AS userid,
        split(value, '::')[1] AS gender,
        split(value, '::')[2] AS age,
        split(value, '::')[3] AS occupation,
        split(value, '::')[4] AS zipcode
    FROM users_staging
""").write.mode("overwrite").saveAsTable("sparkdatalake.users")

# ratings
spark.sql("""
    SELECT
        split(value, '::')[0] AS userid,
        split(value, '::')[1] AS movieid,
        split(value, '::')[2] AS rating,
        split(value, '::')[3] AS timestamp
    FROM ratings_staging
""").write.mode("overwrite").saveAsTable("sparkdatalake.ratings")

# Stop SparkSession
spark.stop()