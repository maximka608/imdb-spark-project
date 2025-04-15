from pyspark.sql import SparkSession
from business_query import *
from schemas import *
from loader import load_data
from dataset_statistic import *


def main():
    session = SparkSession.builder.appName("IMDbLoader").getOrCreate()

    # alternative_titles_df = load_data(session, 'imdb_data/title.akas.tsv', alternative_titles_scheme)
    basic_titles_df = load_data(session, 'imdb_data/title.basics.tsv', basic_titles_scheme)
    # crew_df = load_data(session, 'imdb_data/title.crew.tsv', crew_scheme)
    # episode_df = load_data(session, 'imdb_data/title.episode.tsv', episode_scheme)
    title_cast_and_crew_df = load_data(session, 'imdb_data/title.principals.tsv', title_cast_and_crew_scheme)
    rating_df = load_data(session, 'imdb_data/title.ratings.tsv', rating_scheme)
    # personal_data_df = load_data(session, 'imdb_data/name.basics.tsv', personal_data_scheme)

    # Business queries

    # get_top_rated_films(rating_df, basic_titles_df)
    # get_avg_rating_by_genre(rating_df, basic_titles_df)
    # get_top_hidden_gems_comedy(basic_titles_df, rating_df)
    # get_top3_film_per_year(rating_df, basic_titles_df)
    # get_avg_runtime_minutes_by_genre(basic_titles_df)
    # get_yearly_avg_rating(rating_df, basic_titles_df)


    # Get Statistics

    # get_stats(alternative_titles_df)
    # get_stats(basic_titles_df)
    # get_stats(crew_df)
    # get_stats(episode_df)
    # get_stats(title_cast_and_crew_df)
    # get_stats(rating_df)
    # get_stats(personal_data_df)

if __name__ == "__main__":
    main()