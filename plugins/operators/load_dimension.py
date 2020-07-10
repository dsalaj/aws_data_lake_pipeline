from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

    
class LoadDimensionOperator(BaseOperator):
    
    insert_table_sql = {
        'artists': SqlQueries.artist_table_insert,
        'songs': SqlQueries.song_table_insert,
        'users': SqlQueries.user_table_insert,
        'time': SqlQueries.time_table_insert,
    }

    artist_table_create = ("""
            CREATE TABLE IF NOT EXISTS public.artists (
                artistid varchar(256) NOT NULL,
                name varchar(256),
                location varchar(256),
                lattitude numeric(18,0),
                longitude numeric(18,0)
            )
        """)


    songs_table_create = ("""
            CREATE TABLE IF NOT EXISTS public.songs (
                songid varchar(256) NOT NULL,
                title varchar(256),
                artistid varchar(256),
                "year" int4,
                duration numeric(18,0),
                CONSTRAINT songs_pkey PRIMARY KEY (songid)
            )
        """)

    users_table_create = ("""
            CREATE TABLE IF NOT EXISTS public.users (
                userid int4 NOT NULL,
                first_name varchar(256),
                last_name varchar(256),
                gender varchar(256),
                "level" varchar(256),
                CONSTRAINT users_pkey PRIMARY KEY (userid)
            )
        """)

    time_table_create = ("""
            CREATE TABLE IF NOT EXISTS time (
                start_time VARCHAR(30) PRIMARY KEY,
                hour VARCHAR(30) NOT NULL,
                day VARCHAR(30) NOT NULL,
                week VARCHAR(30) NOT NULL,
                month VARCHAR(30) NOT NULL,
                year VARCHAR(30) NOT NULL,
                weekday BOOLEAN NOT NULL
            )
        """)
    
    create_table_sql = {
        'artists': artist_table_create,
        'songs': songs_table_create,
        'users': users_table_create,
        'time': time_table_create,
    }

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {} LIMIT 100
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id='aws_credentials',
                 table=None,
                 delete_content=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        assert aws_credentials_id is not None
        assert table in ['artists', 'songs', 'users', 'time']
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.aws_credentials_id = aws_credentials_id
        self.delete_content = delete_content

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Creating Redshift table {}".format(self.table))
        redshift.run(LoadDimensionOperator.create_table_sql[self.table])
        
        if self.delete_content:
            self.log.info("Clearing data from destination Redshift table {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from Staging to Dimension table {}".format(self.table))
        redshift.run(LoadDimensionOperator.insert_sql.format(self.table, LoadDimensionOperator.insert_table_sql[self.table]))

        

