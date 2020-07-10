from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                 LoadDimensionOperator, DataQualityOperator)
# from helpers.sql_queries import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'dsalaj',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'email_on_failure': False,
}

dag = DAG('news_trends_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# stage_events_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_events',
#     dag=dag,
#     table='staging_events',
# )
# 
# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag,
#     table='staging_songs',
# )
# 
# start_operator >> stage_events_to_redshift
# start_operator >> stage_songs_to_redshift
# 
# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )
# 
# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table
# 
# load_user_dimension_table = LoadDimensionOperator(
#     table='users',
#     task_id='Load_user_dim_table',
#     dag=dag
# )
# 
# load_song_dimension_table = LoadDimensionOperator(
#     table='songs',
#     task_id='Load_song_dim_table',
#     dag=dag
# )
#  
# load_artist_dimension_table = LoadDimensionOperator(
#     table='artists',
#     task_id='Load_artist_dim_table',
#     dag=dag
# )
# 
# load_time_dimension_table = LoadDimensionOperator(
#     table='time',
#     task_id='Load_time_dim_table',
#     dag=dag
# )
# 
# load_songplays_table >> load_user_dimension_table
# load_songplays_table >> load_song_dimension_table
# load_songplays_table >> load_artist_dimension_table
# load_songplays_table >> load_time_dimension_table
# 
# run_quality_checks = DataQualityOperator(
#     tables=['artists', 'songs', 'users'],
#     task_id='Run_data_quality_checks',
#     dag=dag
# )
# 
# run_quality_checks << load_user_dimension_table
# run_quality_checks << load_song_dimension_table
# run_quality_checks << load_artist_dimension_table
# run_quality_checks << load_time_dimension_table
# 
# end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
# 
# run_quality_checks >> end_operator

