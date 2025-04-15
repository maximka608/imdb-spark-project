from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, count, col, row_number, avg, lower, rank, expr


def get_top_rated_films(title_ratings_df, title_basics_df):
    result = title_ratings_df \
        .filter((col('numVotes') > 1000) & (col('averageRating') > 8.0)) \
        .join(title_basics_df, 'tconst') \
        .select('primaryTitle', 'averageRating', 'numVotes') \
        .orderBy(col('averageRating'), ascending=False)

    result.show(10, truncate=False)
    return result


def get_avg_rating_by_genre(title_ratings_df, title_basics_df):
    df = title_ratings_df.join(title_basics_df, 'tconst')
    genres_split = df.withColumn("genre", F.explode(F.split(F.col("genres"), ",")))

    result = genres_split.groupBy("genre") \
        .agg(F.avg('averageRating').alias('avgRating')) \
        .orderBy(F.col('avgRating'), ascending=False)

    result.show(10, truncate=False)
    return result


def get_top_hidden_gems_comedy(title_basics_df, title_ratings_df):
    movies_with_ratings = title_basics_df.join(title_ratings_df, "tconst")
    comedy_movies = movies_with_ratings.filter(
        (col("genres").contains("Comedy")) &
        (col("averageRating").cast("float") >= 7.0)
    )

    comedy_movies = comedy_movies.withColumn("averageRating", col("averageRating").cast("float"))
    comedy_movies = comedy_movies.withColumn("numVotes", col("numVotes").cast("int"))

    window_rating = Window.orderBy(col("averageRating").desc())
    window_votes = Window.orderBy(col("numVotes").desc())

    comedy_movies = comedy_movies.withColumn("rating_rank", rank().over(window_rating))
    comedy_movies = comedy_movies.withColumn("votes_rank", rank().over(window_votes))

    comedy_movies = comedy_movies.withColumn("hidden_gem_score", col("rating_rank") + col("votes_rank"))

    result = comedy_movies.select(
        "primaryTitle",
        "originalTitle",
        "startYear",
        "averageRating",
        "numVotes",
        "genres",
        "hidden_gem_score"
    ).orderBy(col("hidden_gem_score").desc())

    result.show(10, truncate=False)
    return result

def get_top3_film_per_year(title_ratings_df, title_basics_df):
    joined = title_ratings_df.join(title_basics_df, "tconst")
    joined_filtered = joined.filter(col('startYear').isNotNull())
    window_spec = Window.partitionBy("startYear").orderBy(col("numVotes").desc())

    result = joined_filtered.withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") <= 3).select("primaryTitle", 'startYear', "numVotes", 'genres', 'rank').orderBy(col("startYear").desc())

    result.show(10, truncate=False)
    return result


def get_avg_runtime_minutes_by_genre(title_basics_df):
    genres_split = title_basics_df.withColumn("genre", F.explode(F.split(F.col("genres"), ",")))
    filter_df = genres_split.filter((col("genre") != "\\N") & (col("runtimeMinutes").isNotNull()))
    result = filter_df.groupBy('genre') \
        .agg(avg('runtimeMinutes').alias('avgRuntimeMinutes')) \
        .orderBy(col('avgRuntimeMinutes').desc()) \
        .select('genre', 'avgRuntimeMinutes')

    result.show(10, truncate=False)
    return result


def get_yearly_avg_rating(title_ratings_df, title_basics_df):
    result = title_ratings_df.join(title_basics_df, "tconst") \
        .filter((col("titleType") == "movie") & col('startYear').isNotNull()) \
        .groupBy("startYear") \
        .agg(avg("averageRating").alias("yearly_avg_rating")) \
        .orderBy(col("startYear").desc())

    result.show(10, truncate=False)
    return result
