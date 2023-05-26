from fastapi import APIRouter
from spark_apps.sql import \
        movies_tasks, create_database_tables,\
        ratings_tasks

create_obj = create_database_tables.CreateDatabase()
spark_obj = create_obj.create_database()
router = APIRouter()


movie_obj = movies_tasks.MovieTasks(spark_obj)
ratings_obj = ratings_tasks.RatingsTasks(spark_obj)

@router.get("/sql/oldest_movies")
def read_root():

   
    return movie_obj.get_oldest_movies()

@router.get("/sql/get_movies_count_by_year")
def read_root():

    return movie_obj.get_movies_count_by_year()

@router.get("/sql/get_movies_count_by_ratings")
def read_root():

    return ratings_obj.get_movies_count_by_ratings()

@router.get("/sql/get_users_count_by_movies")
def read_root():

    return ratings_obj.get_users_per_movie()

@router.get("/sql/get_total_ratings_per_movie")
def read_root():

    return ratings_obj.get_total_ratings_per_movie()

@router.get("/sql/get_avg_ratings_per_movie")
def read_root():

    return ratings_obj.get_avg_ratings_per_movie()