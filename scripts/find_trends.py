import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, concat_ws, trim, asc, udf, explode, col
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
import spacy


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


LOCAL = os.getenv('RUN_LOCAL', False)
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

spark = create_spark_session()
s3_path = 's3a://udacity-s3-emr/'
if LOCAL:
    s3_path = '.'

file_path = os.path.join(s3_path, 'titles.parquet')

df_titles = spark.read.parquet(file_path)
df_titles = df_titles.withColumn("date", to_date(concat_ws("-", "year", "month", "day")))
df_titles = df_titles.withColumn("title", trim(col("title")))
#df_titles = df_titles.select(["title", "date"])
if LOCAL:
    df_titles.printSchema()
    df_titles.show(n=10)

df_date = df_titles.groupBy("date").count().sort(asc("date"))
if LOCAL:
    df_date.printSchema()
    df_date.show(n=15)


def title_to_named_entity_list(title):
    nlp = spacy.load("en_core_web_sm")
    doc = nlp(title)
    return [(str(ent.text), str(ent.label_)) for ent in doc.ents]


udf_title_to_named_entity_list = udf(
    title_to_named_entity_list,
    ArrayType(StructType([
        StructField("text", StringType(), False),
        StructField("label", StringType(), False)
    ]))
)

if LOCAL:
    df_titles = df_titles.limit(200)
df_ner = df_titles.withColumn("ner", udf_title_to_named_entity_list("title"))
df_ner = df_ner.withColumn("ner", explode("ner"))
# List of all entity labels: https://spacy.io/api/annotation#named-entities
exclude_types = ['DATE', 'TIME', 'PERCENT', 'MONEY', 'QUANTITY', 'ORDINAL', 'CARDINAL']
df_ner = df_ner.filter(~df_ner.ner.label.isin(exclude_types))
if LOCAL:
    df_ner.printSchema()
    df_ner.show(20)

out_path = os.path.join(s3_path, 'fact_ner.parquet')
df_ner.write \
    .partitionBy(["year", "month", "day"]) \
    .format("parquet") \
    .mode("overwrite") \
    .save(out_path)
