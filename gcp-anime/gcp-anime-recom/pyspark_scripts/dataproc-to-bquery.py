import pyspark

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType
import pyspark.sql.functions as F

def list_null_ids(df, col_name):
  return df.filter((F.col(col_name).isNull()) | (F.col(col_name) == 'Unknown'))\
        .select(F.col(df.columns[0]))\
      .rdd\
      .flatMap(lambda x: x)\
      .collect()

def collect_null_rows(df):
    null_ids = []
    for c in df.columns:
        if c == 'rating' or c.startswith('score'):
            ids = list_null_ids(df, c)
            null_ids.extend(ids)
    return list(set(null_ids))

def clean_null_score(df):
  return df.filter(~df[df.columns[0]].isin(collect_null_rows(df)))

def replace_unknown(df):
  for c in df.columns:
    if not c == 'rating' or not c.startswith('score'):
      df = df.withColumn(c, F.when(df[c] == 'Unknown', None).otherwise(df[c]))
  return df

temp_bucket = 'pyspark-temp-vk'

import_bucket = 'gs://anime-data/preprocessed'
export_bucket = 'gs://anime-data/processed'


spark = SparkSession \
       .builder \
       .getOrCreate()

spark.conf.set('temporaryGcsBucket', temp_bucket)

# Import raw data

# -------------------------------
# DEFINE SCHEMA
# -------------------------------

anime_raw_schema = StructType([
    StructField("mal_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("score", FloatType(), True),
    StructField("genres", StringType(), True),
    StructField("english_name", StringType(), True),
    StructField("japanese_name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("episodes", IntegerType(), True),
    StructField("aired", StringType(), True),
    StructField("premiered", StringType(), True),
    StructField("producers", StringType(), True),
    StructField("licensors", StringType(), True),
    StructField("studios", StringType(), True),
    StructField("sources", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("ranked", IntegerType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("members", IntegerType(), True),
    StructField("favorites", IntegerType(), True),
    StructField("watching", IntegerType(), True),
    StructField("completed", IntegerType(), True),
    StructField("on_hold", IntegerType(), True),
    StructField("dropped", IntegerType(), True),
    StructField("plan_to_watch", IntegerType(), True),
    StructField("score_10", IntegerType(), True),
    StructField("score_9", IntegerType(), True),
    StructField("score_8", IntegerType(), True),
    StructField("score_7", IntegerType(), True),
    StructField("score_6", IntegerType(), True),
    StructField("score_5", IntegerType(), True),
    StructField("score_4", IntegerType(), True),
    StructField("score_3", IntegerType(), True),
    StructField("score_2", IntegerType(), True),
    StructField("score_1", IntegerType(), True)
])

anime_with_synopsis_raw_schema = StructType([
    StructField("mal_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("score", FloatType(), True),
    StructField("genres", StringType(), True),
    StructField("synopsis", StringType(), True)
])

animelist_raw_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("anime_id", IntegerType(), False),
    StructField("rating", IntegerType(), True),
    StructField("watching_status", IntegerType(), True),
    StructField("watched_episodes", IntegerType(), True)
])

rating_complete_raw_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("anime_id", StringType(), True),
    StructField("rating", FloatType(), True)
])

watching_status_schema = StructType([
    StructField("status", IntegerType(), False),
    StructField("description", StringType(), True)
])

# -------------------------------
# IMPORT DATA
# -------------------------------

anime_raw_df = spark.read\
          .schema(anime_raw_schema)\
          .parquet(f'{import_bucket}/anime')

anime_with_synopsis_raw_df = spark.read\
          .schema(anime_with_synopsis_raw_schema)\
          .parquet(f'{import_bucket}/anime_with_synopsis')

animelist_raw_df = spark.read\
          .schema(animelist_raw_schema)\
          .parquet(f'{import_bucket}/animelist')

rating_complete_raw_df = spark.read\
    .parquet(f'{import_bucket}/rating_complete')

watching_status_df = spark.read\
    .parquet(f'{import_bucket}/watching_status')

# -------------------------------
# CLEAN DATA
# -------------------------------


anime_clean_df = clean_null_score(anime_raw_df)\
                    .withColumnRenamed('score', 'avg_rating')\
                    .withColumnRenamed('rating', 'age_rating')

anime_clean_df = replace_unknown(anime_clean_df)

anime_with_synopsis_clean_df = clean_null_score(anime_with_synopsis_raw_df)\
                                    .withColumnRenamed('score', 'avg_rating')\
                                    .withColumnRenamed('rating', 'age_rating')

anime_with_synopsis_clean_df = replace_unknown(anime_with_synopsis_clean_df)

# -------------------------------
# PREPARE FOR EXPORT
# -------------------------------


anime_synopsis_df = anime_with_synopsis_clean_df.select('mal_id', 'synopsis')

anime_details_df = anime_clean_df.select('mal_id', 'name', 'genres', 'english_name',
                                         'japanese_name', 'type', 'episodes',
                                         'aired', 'premiered', 'producers',
                                         'licensors', 'studios', 'sources',
                                         'duration', 'age_rating', 'ranked')

anime_stats_df = anime_clean_df.select('mal_id', 'popularity', 'members', 'favorites',
                                       'watching', 'completed', 'on_hold', 'dropped',
                                       'plan_to_watch')

animelist_df = animelist_raw_df.withColumn('anime_completed',
                       F.when((F.col('watching_status') != 2) | (F.col('rating') == 0), 'N')
                       .otherwise('Y'))

anime_fact_df = animelist_df\
                  .join(F.broadcast(anime_clean_df),
                        animelist_df['anime_id'] == anime_clean_df['mal_id'],
                        'left')\
                  .select('user_id', 'anime_id', 'rating',
                          'watching_status', 'watched_episodes', 'avg_rating',
                          'anime_completed', 'score_10', 'score_9', 'score_8',
                          'score_7', 'score_6', 'score_5', 'score_4',
                          'score_3', 'score_2', 'score_1')

# -------------------------------
# EXPORT TO GCS
# -------------------------------

anime_fact_df.write.parquet(f'{export_bucket}/anime_fact', mode='overwrite')

anime_details_df.write.parquet(f'{export_bucket}/anime_details', mode='overwrite')

anime_stats_df.write.parquet(f'{export_bucket}/anime_stats', mode='overwrite')

anime_synopsis_df.write.parquet(f'{export_bucket}/anime_synopsis', mode='overwrite')

watching_status_df.write.parquet(f'{export_bucket}/watching_status', mode='overwrite')

# -------------------------------
# EXPORT TO BIGQUERY
# -------------------------------

anime_fact_df.write\
    .format('bigquery')\
    .option('table', 'gcp-anime-recom.animepet.animeFact')\
    .mode('overwrite')\
    .save()

anime_details_df.write\
    .format('bigquery')\
    .option('table', 'gcp-anime-recom.animepet.animeDetails')\
    .mode('overwrite')\
    .save()

anime_stats_df.write\
    .format('bigquery')\
    .option('table', 'gcp-anime-recom.animepet.animeStats')\
    .mode('overwrite')\
    .save()

anime_synopsis_df.write\
    .format('bigquery')\
    .option('table', 'gcp-anime-recom.animepet.animeSynopsis')\
    .mode('overwrite')\
    .save()

watching_status_df.write\
    .format('bigquery')\
    .option('table', 'gcp-anime-recom.animepet.watchingStatus')\
    .mode('overwrite')\
    .save()

spark.stop()