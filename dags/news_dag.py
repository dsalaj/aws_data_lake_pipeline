import os
import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import MyCustomOperator
from airflow.operators import AWSEMROperator
# from airflow.operators.my_custom_operator import MyCustomOperator
# from airflow.operators.aws_emr_etl import AWSEMROperator

# from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor

# from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                 LoadDimensionOperator, DataQualityOperator)
# from helpers.sql_queries import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

local_tz = pendulum.timezone("Europe/Amsterdam")

default_args = {
    'owner': 'dsalaj',
    'start_date': datetime(2020, 7, 10, 0, 0, 0, tzinfo=local_tz),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'email_on_failure': False,
    'catchup': False,
    'max_active_runs': 1,
}

DAG_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
AWS_CONN_ID = "aws_credentials"

dag = DAG('news_dag',
          default_args=default_args,
          description='Load and transform data in S3 using EMR',
          schedule_interval='@monthly',
          catchup=False,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

def print_context(ds, **kwargs):
    print(ds)
    return 'Whatever you return gets printed in the logs'


test_py_op = PythonOperator(
    task_id='python_print_task',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

start_operator >> test_py_op

custom_plugin_op = MyCustomOperator(
    task_id='custom_plugin_op',
    provide_context=True,
    dag=dag,
)

test_py_op >> custom_plugin_op

aws_emr_etl_operator = AWSEMROperator(
    task_id="create_EMR_cluster_and_execute_ETL",
    dag=dag,
    conn_id=AWS_CONN_ID,
    time_zone=local_tz,
)

custom_plugin_op >> aws_emr_etl_operator

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

