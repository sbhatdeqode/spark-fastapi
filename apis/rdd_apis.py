from fastapi import APIRouter
from apis.sql_apis import spark_obj
from spark_apps.rdd import \
    most_viewed_movies, distinct_genres, movies_by_genres, \
    movies_starts_with_numbers_letters, latest_movies


router = APIRouter()

@router.get("/rdd/most_watched_movies")
def read_root():

    obj = most_viewed_movies.MostViewedMovies(spark_obj)
    return obj.get_most_viewed_movies()

@router.get("/rdd/distinct_genres")
def read_root():

    obj = distinct_genres.DistinctGenres()
    return obj.get_distinct_genres()

@router.get("/rdd/movies_by_genres")
def read_root():

    obj = movies_by_genres.MoviesCountByGenres()
    return obj.get_movies_count()

@router.get("/rdd/movies_starts_with_letter_or_number")
def read_root():

    obj = movies_starts_with_numbers_letters.MoviesNumbersLetters()
    return obj.get_movies_count()

@router.get("/rdd/latest_movies")
def read_root():

    obj = latest_movies.LatestMovies()
    return obj.get_latest_movies()