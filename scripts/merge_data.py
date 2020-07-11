import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, to_timestamp, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType, DateType


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


spark = create_spark_session()
s3_path = 's3a://udacity-s3-emr/'

file_path = s3_path + 'all-the-news-2-1_colsfiltered.csv'
news_schema = StructType([
    StructField('year', IntegerType()),
    StructField('month', IntegerType()),
    StructField('day', IntegerType()),
    StructField('title', StringType()),
])
df = spark.read.csv(file_path, inferSchema=True, header=True)

df = df.withColumn("day", df.day.cast(IntegerType()))
df = df.withColumn("month", df.month.cast(IntegerType()))
df = df.withColumn("year", df.year.cast(IntegerType()))
df = df.na.drop(subset=["year", "month", "day", "title"])

news_2016 = df[df.year==2016]

file_path = s3_path + 'RS_filtered.csv'
reddit_schema = StructType([
    StructField('_c0', StringType()),
    StructField('num_comments', IntegerType()),
    StructField('title', StringType()),
    StructField('created_utc', IntegerType()),
])
df = spark.read.csv(file_path, inferSchema=True, header=True, schema=reddit_schema)


df = df.withColumn('date', to_timestamp(df.created_utc))
df = df.withColumn("day", dayofmonth(df.date))
df = df.withColumn("month", month(df.date))
df = df.withColumn("year", year(df.date))

df = df.select(["year", "month", "day", "title", "num_comments"])
df = df.na.drop(subset=["year", "month", "day", "title"])


reddit_2016 = df[df.year==2016]
reddit_2016 = reddit_2016[reddit_2016.num_comments > 10]

union_2016 = reddit_2016.select(["year", "month", "day", "title"]).union(news_2016)

out_path = os.path.join(s3_path, 'titles.parquet')
union_2016.write \
    .partitionBy('year', "month", "day") \
    .format("parquet") \
    .save(out_path)