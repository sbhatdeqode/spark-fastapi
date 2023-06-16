"""
    api end-points for sql solutions. 
"""

from fastapi import APIRouter
from spark_apps.sql import \
        movies_tasks, create_database_tables,\
        ratings_tasks


router = APIRouter()

create_obj = create_database_tables.CreateDatabase()
spark_obj = create_obj.create_database()
movie_obj = movies_tasks.MovieTasks(spark_obj)
ratings_obj = ratings_tasks.RatingsTasks(spark_obj)


@router.get("/sql/oldest_movies")
def oldest_movies():

    """
        api end-point to get oldest_movies. 
    """

    return movie_obj.get_oldest_movies()


@router.get("/sql/get_movies_count_by_year")
def movies_count_by_year():

    """
        api end-point to get movies_count_by_year. 
    """

    return movie_obj.get_movies_count_by_year()


@router.get("/sql/get_movies_count_by_ratings")
def movies_count_by_ratings():

    """
        api end-point to get movies_count_by_ratings. 
    """

    return ratings_obj.get_movies_count_by_ratings()


@router.get("/sql/get_users_count_by_movies")
def users_count_by_movies():

    """
        api end-point to get users_per_movie. 
    """

    return ratings_obj.get_users_per_movie()


@router.get("/sql/get_total_ratings_per_movie")
def total_ratings_per_movie():

    """
        api end-point to get total_ratings_per_movie. 
    """

    return ratings_obj.get_total_ratings_per_movie()


@router.get("/sql/get_avg_ratings_per_movie")
def avg_ratings_per_movie():

    """
        api end-point to get avg_ratings_per_movie. 
    """

    return ratings_obj.get_avg_ratings_per_movie()
