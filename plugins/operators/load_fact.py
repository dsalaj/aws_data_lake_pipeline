from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO songplays
        {} LIMIT 100
    """

    #md5(events.sessionid || events.start_time) songplay_id,
    songplay_table_insert = ("""
            SELECT
                md5(events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
            FROM
                (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, * 
                 FROM staging_events
                 WHERE page='NextSong'
                 AND ts IS NOT NULL AND ts > 0
                 AND userid IS NOT NULL) events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    songplays_table_create = ("""
            CREATE TABLE IF NOT EXISTS public.songplays (
                playid varchar(32) NOT NULL,
                start_time timestamp NOT NULL,
                userid int4 NOT NULL,
                "level" varchar(256),
                songid varchar(256),
                artistid varchar(256),
                sessionid int4,
                location varchar(256),
                user_agent varchar(256),
                CONSTRAINT songplays_pkey PRIMARY KEY (playid)
            )
        """)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id='aws_credentials',
                 delete_content=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        assert aws_credentials_id is not None
        self.redshift_conn_id = redshift_conn_id
        self.table='songplays'
        self.aws_credentials_id = aws_credentials_id
        self.delete_content = delete_content

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Creating Redshift table {}".format(self.table))
        redshift.run(LoadFactOperator.songplays_table_create)
        
        if self.delete_content:
            self.log.info("Clearing data from destination Redshift table {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from Staging to Fact table {}".format(self.table))
        redshift.run(LoadFactOperator.insert_sql.format(LoadFactOperator.songplay_table_insert))

        
        