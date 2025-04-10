from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType, ArrayType

alternative_titles_scheme = StructType([
    StructField('titleId', StringType(), True),
    StructField('ordering', IntegerType(), True),
    StructField('title', StringType(), True),
    StructField('region', StringType(), True),
    StructField('language', StringType(), True),
    StructField('types', StringType(), True),
    StructField('attributes', StringType(), True),
    StructField('isOriginalTitle', BooleanType(), True)
])

basic_titles_scheme = StructType([
    StructField('tconst', StringType(), True),
    StructField('titleType', StringType(), True),
    StructField('primaryTitle', StringType(), True),
    StructField('originalTitle', StringType(), True),
    StructField('isAdult', BooleanType(), True),
    StructField('startYear', IntegerType(), True),
    StructField('endYear', IntegerType(), True),
    StructField('runtimeMinutes', IntegerType(), True),
    StructField('genres', StringType(), True)
])

crew_scheme = StructType([
    StructField('tconst', StringType(), True),
    StructField('directors', StringType(), True),
    StructField('writers', StringType(), True)
])

episode_scheme = StructType([
    StructField('tconst', StringType(), True),
    StructField('parentTconst', StringType(), True),
    StructField('seasonNumber', IntegerType(), True),
    StructField('episodeNumber', IntegerType(), True)
])

title_cast_and_crew_scheme = StructType([
    StructField('tconst', StringType(), True),
    StructField('ordering', IntegerType(), True),
    StructField('nconst', StringType(), True),
    StructField('category', StringType(), True),
    StructField('job', StringType(), True),
    StructField('characters', StringType(), True)
])

rating_scheme = StructType([
    StructField('tconst', StringType(), True),
    StructField('averageRating', FloatType(), True),
    StructField('numVotes', IntegerType(), True)
])

personal_data_scheme = StructType([
    StructField('nconst', StringType(), True),
    StructField('primaryName', StringType(), True),
    StructField('birthYear', IntegerType(), True),
    StructField('deathYear', IntegerType(), True),
    StructField('primaryProfession', StringType(), True),
    StructField('knownForTitles', StringType(), True)
])

