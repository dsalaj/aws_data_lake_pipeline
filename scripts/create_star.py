import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, to_date, concat_ws, trim, asc, udf, explode, col
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

file_path = os.path.join(s3_path, 'fact_ner.parquet')

df_facts = spark.read.parquet(file_path)


def extract_dimension_df_from_column(df, column):
    return df.select(column).distinct().withColumn('id', monotonically_increasing_id())


df_titles = extract_dimension_df_from_column(df_facts, 'title')
df_date = extract_dimension_df_from_column(df_facts, 'date')
df_ner = extract_dimension_df_from_column(df_facts, 'ner')
dim_dfs = {
    'title': df_titles,
    'date': df_date,
    'ner': df_ner,
}

if LOCAL:
    for dim_df in dim_dfs.values():
        dim_df.printSchema()
        dim_df.show(n=10)

for col_name, dim_df in dim_dfs.items():
    df_facts = df_facts.join(dim_df.selectExpr(f'{col_name}', f'id as {col_name}_id'), col_name, how='left')

df_facts = df_facts.select([f'{col_name}_id' for col_name in dim_dfs.keys()])

if LOCAL:
    df_facts.printSchema()
    df_facts.show(n=15)


for col_name, dim_df in dim_dfs.items():
    out_path = os.path.join(s3_path, f'dim_{col_name}.csv')
    dim_df.repartition(1).write \
        .mode("overwrite") \
        .csv(out_path)

out_path = os.path.join(s3_path, f'fact_news.csv')
df_facts.repartition(1).write \
        .mode("overwrite") \
        .csv(out_path)

    # dim_df.write.format("com.databricks.spark.redshift") \
    #     .option("url", "jdbc:redshift://news-nlp-redshift-2020-07-26-14-58.cn1rk9cypivi.us-west-2.redshift.amazonaws.com:5439/newsstar") \
    #     .option("dbtable", col_name) \
    #     .option("tempdir", os.path.join(s3_path, 'tmp')) \
    #     .mode("overwrite") \
    #     .save()

# https://aws.amazon.com/about-aws/whats-new/2018/06/amazon-redshift-can-now-copy-from-parquet-and-orc-file-formats/
# https://stackoverflow.com/questions/48162952/moving-a-dataframe-to-redshift-using-pyspark