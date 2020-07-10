from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    staging_events_table_create = ("""
            CREATE TABLE IF NOT EXISTS public.staging_events (
                artist varchar(256),
                auth varchar(256),
                firstname varchar(256),
                gender varchar(256),
                iteminsession int4,
                lastname varchar(256),
                length numeric(18,0),
                "level" varchar(256),
                location varchar(256),
                "method" varchar(256),
                page varchar(256),
                registration numeric(18,0),
                sessionid int4,
                song varchar(256),
                status int4,
                ts int8,
                useragent varchar(256),
                userid int4
            )
        """)

    staging_songs_table_create = ("""
            CREATE TABLE IF NOT EXISTS public.staging_songs (
                num_songs int4,
                artist_id varchar(256),
                artist_name varchar(256),
                artist_latitude numeric(18,0),
                artist_longitude numeric(18,0),
                artist_location varchar(256),
                song_id varchar(256),
                title varchar(256),
                duration numeric(18,0),
                "year" int4
            )
        """)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table=None,
                 s3_bucket_key='',
                 aws_credentials_id='aws_credentials',
                 region='us-west-2',
                 path=None,
                 delimiter=',',
                 headers='1',
                 quote_char='"',
                 file_type='json',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        assert table in ['staging_events', 'staging_songs']
        assert aws_credentials_id is not None
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket_key = s3_bucket_key
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.path = path
        self.delimiter = delimiter
        self.headers = headers
        self.quote_char = quote_char
        self.file_type = file_type

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Creating Redshift table {}".format(self.table))
        redshift.run(StageToRedshiftOperator.staging_events_table_create if self.table == 'staging_events' else
                     StageToRedshiftOperator.staging_songs_table_create)

        self.log.info("Clearing data from destination Redshift table {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                REGION AS '{}'
        """.format(self.table, self.s3_bucket_key, credentials.access_key, credentials.secret_key, self.region)

        if self.file_type == 'csv':
            file_statement = """
                        delimiter '{}'
                        ignoreheader {}
                        csv quote as '{}';
                    """.format(self.delimiter, self.headers, self.quote_char)
        elif self.file_type == 'json':
            file_statement = "JSON 'auto'" if self.path is None else "JSON '{}'".format(self.path)
        else:
            raise ValueError("Unknown file type {}".format(self.file_type))

        full_copy_statement = '{} {}'.format(copy_sql, file_statement)

        self.log.info("Copying data from S3 to Redshift")
        redshift.run(full_copy_statement)

        self.log.info('StageToRedshiftOperator executed for table {}'.format(self.table))
