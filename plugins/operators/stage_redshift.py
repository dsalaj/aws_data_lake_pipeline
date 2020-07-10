from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        JSON 'auto'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """
    
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
                 aws_credentials_id='aws_credentials',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        assert table in ['staging_events', 'staging_songs']
        assert aws_credentials_id is not None
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
      
        self.log.info("Creating Redshift table {}".format(self.table))
        redshift.run(StageToRedshiftOperator.staging_events_table_create if self.table == 'staging_events' else
                     StageToRedshiftOperator.staging_songs_table_create)

        self.log.info("Clearing data from destination Redshift table {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://udacity-dend/log_data" if self.table == 'staging_events' else 's3://udacity-dend/song_data'
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
        )
        redshift.run(formatted_sql)
        
        self.log.info('StageToRedshiftOperator executed for table {}'.format(self.table))
