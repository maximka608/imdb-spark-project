from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType, ArrayType

title_akas_scheme = StructType([
    StructField('titleId', StringType(), True),
    StructField('ordering', IntegerType(), True),
    StructField('title', StringType(), True),
    StructField('region', StringType(), True),
    StructField('language', StringType(), True),
    StructField('types', StringType(), True),
    StructField('attributes', StringType(), True),
    StructField('isOriginalTitle', BooleanType(), True)
])

title_basics_scheme = StructType([
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

title_crew_scheme = StructType([
    StructField('tconst', StringType(), True),
    StructField('directors', StringType(), True),
    StructField('writers', StringType(), True)
])

title_episode_scheme = StructType([
    StructField('tconst', StringType(), True),
    StructField('parentTconst', StringType(), True),
    StructField('seasonNumber', IntegerType(), True),
    StructField('episodeNumber', IntegerType(), True)
])

title_principals_scheme = StructType([
    StructField('tconst', StringType(), True),
    StructField('ordering', IntegerType(), True),
    StructField('nconst', StringType(), True),
    StructField('category', StringType(), True),
    StructField('job', StringType(), True),
    StructField('characters', StringType(), True)
])

title_ratings_scheme = StructType([
    StructField('tconst', StringType(), True),
    StructField('averageRating', FloatType(), True),
    StructField('numVotes', IntegerType(), True)
])

name_basics_scheme = StructType([
    StructField('nconst', StringType(), True),
    StructField('primaryName', StringType(), True),
    StructField('birthYear', IntegerType(), True),
    StructField('deathYear', IntegerType(), True),
    StructField('primaryProfession', StringType(), True),
    StructField('knownForTitles', StringType(), True)
])

