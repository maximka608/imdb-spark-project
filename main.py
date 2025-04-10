from pyspark.sql import SparkSession
from schemas import *
from loader import load_data, print_data_schema


def main():
    session = SparkSession.builder.appName("IMDbLoader").getOrCreate()

    alternative_titles_df = load_data(session, 'imdb_data/title.akas.tsv', alternative_titles_scheme)
    basic_titles_df = load_data(session, 'imdb_data/title.basics.tsv', basic_titles_scheme)
    crew_df = load_data(session, 'imdb_data/title.crew.tsv', crew_scheme)
    episode_df = load_data(session, 'imdb_data/title.episode.tsv', episode_scheme)
    title_cast_and_crew_df = load_data(session, 'imdb_data/title.principals.tsv', title_cast_and_crew_scheme)
    rating_df = load_data(session, 'imdb_data/title.ratings.tsv', rating_scheme)
    personal_data_df = load_data(session, 'imdb_data/name.basics.tsv', personal_data_scheme)

    print_data_schema(alternative_titles_df)
    print_data_schema(basic_titles_df)
    print_data_schema(crew_df)
    print_data_schema(episode_df)
    print_data_schema(title_cast_and_crew_df)
    print_data_schema(rating_df)
    print_data_schema(personal_data_df)

if __name__ == "__main__":
    main()