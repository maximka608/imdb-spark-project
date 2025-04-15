from pyspark.sql import SparkSession

from utils.business_query import *
from utils.record_writer import write_data_to_csv
from utils.schemas import *
from utils.loader import load_data


def main():
    session = SparkSession.builder.appName("IMDbLoader").getOrCreate()

    # alternative_titles_df = load_data(session, 'imdb_data/title.akas.tsv', alternative_titles_scheme)
    basic_titles_df = load_data(session, 'imdb_data/title.basics.tsv', basic_titles_scheme)
    # crew_df = load_data(session, 'imdb_data/title.crew.tsv', crew_scheme)
    # episode_df = load_data(session, 'imdb_data/title.episode.tsv', episode_scheme)
    title_cast_and_crew_df = load_data(session, 'imdb_data/title.principals.tsv', title_cast_and_crew_scheme)
    rating_df = load_data(session, 'imdb_data/title.ratings.tsv', rating_scheme)
    # personal_data_df = load_data(session, 'imdb_data/name.basics.tsv', personal_data_scheme)


    # Get Statistics

    # get_stats(alternative_titles_df)
    # get_stats(basic_titles_df)
    # get_stats(crew_df)
    # get_stats(episode_df)
    # get_stats(title_cast_and_crew_df)
    # get_stats(rating_df)
    # get_stats(personal_data_df)


    # Business queries

    top_rated_films = get_top_rated_films(rating_df, basic_titles_df)
    avg_rating_genre = get_avg_rating_by_genre(rating_df, basic_titles_df)
    comedy_hidden_gems = get_top_hidden_gems_comedy(basic_titles_df, rating_df)
    top3_film_per_year = get_top3_film_per_year(rating_df, basic_titles_df)
    avg_runtime_by_genre = get_avg_runtime_minutes_by_genre(basic_titles_df)
    yearly_rating = get_yearly_avg_rating(rating_df, basic_titles_df)

    write_data_to_csv(top_rated_films, 'records/top_rated_films.csv')
    write_data_to_csv(avg_rating_genre, 'records/avg_rated_films.csv')
    write_data_to_csv(comedy_hidden_gems, 'records/comedy_hidden_gems.csv')
    write_data_to_csv(top3_film_per_year, 'records/top3_film_per_year.csv')
    write_data_to_csv(avg_runtime_by_genre, 'records/avg_runtime_by_genre.csv')
    write_data_to_csv(yearly_rating, 'records/yearly_rating.csv')


if __name__ == "__main__":
    main()