import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, regexp_replace, col


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


# Remove Special characters
df_facts = df_facts.withColumn('title', regexp_replace(col('title'), "/[^a-zA-Z0-9 -_]+/", ""))


df_titles = extract_dimension_df_from_column(df_facts, 'title')
df_date = extract_dimension_df_from_column(df_facts, 'date')
df_ner = extract_dimension_df_from_column(df_facts, 'ner')
# Split struct column "ner" to simple string columns: text, label
df_ner = df_ner.select('id', 'ner', col('ner.*'))
# Remove Special characters
df_ner = df_ner.withColumn('text', regexp_replace(col('text'), "[^a-zA-Z0-9 -_]+", ""))

dim_dfs = {
    'title': df_titles,
    'date': df_date,
    'ner': df_ner,
}

for col_name, dim_df in dim_dfs.items():
    df_facts = df_facts.join(dim_df.selectExpr(f'{col_name}', f'id as {col_name}_id'), col_name, how='left')

# Keep only foreign keys to dimension tables
df_facts = df_facts.select([f'{col_name}_id' for col_name in dim_dfs.keys()])

# Drop redundant struct field as it is not compatible with CSV
dim_dfs['ner'] = dim_dfs['ner'].drop('ner')


if LOCAL:
    for dim_df in dim_dfs.values():
        dim_df.printSchema()
        dim_df.show(n=10)

    df_facts.printSchema()
    df_facts.show(n=15)

for col_name, dim_df in dim_dfs.items():
    out_path = os.path.join(s3_path, f'dim_{col_name}.csv')
    dim_df.repartition(1).write \
        .option("header", "true") \
        .option("quote", "`") \
        .mode("overwrite") \
        .csv(out_path)

out_path = os.path.join(s3_path, f'fact_news.csv')
df_facts.repartition(1).write \
    .option("header", "true") \
    .option("quote", "`") \
    .mode("overwrite") \
    .csv(out_path)
