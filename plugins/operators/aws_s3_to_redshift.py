import os

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook


class S3ToRedshiftTransfer(BaseOperator):
    """
        Copy data from S3 to Redshift.
    """

    ui_color = "#FF5566"

    query_dict = {
        'dim_date': """
            CREATE TABLE IF NOT EXISTS dim_date (
              date DATE NOT NULL,
              id VARCHAR PRIMARY KEY
            )
        """,
        'dim_title': """
        CREATE TABLE IF NOT EXISTS dim_title (
            title VARCHAR NOT NULL,
            id VARCHAR PRIMARY KEY
        )
        """,
        'dim_ner': """
        CREATE TABLE IF NOT EXISTS dim_ner (
            id VARCHAR PRIMARY KEY,
            text VARCHAR NOT NULL,
            label VARCHAR NOT NULL
        )
        """,
        'fact_news': """
        CREATE TABLE IF NOT EXISTS fact_news (
            title_id VARCHAR NOT NULL,
            date_id VARCHAR NOT NULL,
            ner_id VARCHAR NOT NULL,
            PRIMARY KEY(title_id, date_id, ner_id)
        )
        """,
    }

    @apply_defaults
    def __init__(self,
                 schema,
                 table,
                 s3_bucket,
                 s3_key,
                 redshift_conn_id,
                 aws_conn_id,
                 time_zone=None,
                 copy_options=tuple(),
                 *args,
                 **kwargs):
        super(S3ToRedshiftTransfer, self).__init__(*args, **kwargs)
        self.schema = schema
        assert table in self.query_dict.keys(), f"Unknown table, defined: {list(self.query_dict.keys())}"
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.time_zone = time_zone
        self.copy_options = copy_options
        self.region = os.environ.get('AWS_DEFAULT_REGION')
        self.s3_bucket = os.environ.get('AWS_S3_BUCKET')

    def execute(self, context):
        self.log.info("Initialize AWS connection ...")
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        copy_options = '\n\t\t\t'.join(self.copy_options)
        arn_role = context['task_instance'].xcom_pull('create_redshift_cluster', key='return_value')[1]

        copy_query = """
            COPY public.{table}
            FROM 's3://{s3_bucket}/{s3_key}'
            iam_role '{aws_iam_role}'
            {copy_options}
            REGION AS '{region}';
        """.format(table=self.table,
                   s3_bucket=self.s3_bucket,
                   s3_key=self.s3_key,
                   access_key=credentials.access_key,
                   secret_key=credentials.secret_key,
                   aws_iam_role=arn_role,
                   copy_options=copy_options,
                   region=self.region)

        self.log.info('Executing COPY command...')
        redshift.run(self.query_dict[self.table])
        redshift.run(copy_query)
        self.log.info("COPY command complete...")
