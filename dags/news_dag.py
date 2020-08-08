import os
import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import MyCustomOperator
from airflow.operators import AWSEMROperator
from airflow.operators import AWSS3UploadOperator
from airflow.operators import AWSRedshiftOperator
from airflow.operators import S3ToRedshiftTransfer

from airflow.contrib.sensors.aws_redshift_cluster_sensor import AwsRedshiftClusterSensor
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
# from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer

# from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator


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
    'retries': 0,  # FIXME: change to 1 later
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'email_on_failure': False,
    'catchup': False,
    'max_active_runs': 1,
}

DAG_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
AWS_CONN_ID = "aws_credentials"
AWS_REDSHIFT_CONN_ID = "aws_redshift_db"

dag = DAG('news_dag',
          default_args=default_args,
          description='Load and transform data in S3 using EMR',
          schedule_interval='@monthly',
          catchup=False,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

scripts_path = "scripts"
aws_s3_upload_scripts = AWSS3UploadOperator(
    task_id="upload_scripts_to_S3",
    dag=dag,
    file_names=[os.path.join(scripts_path, scriptname) for scriptname in os.listdir(scripts_path)],
    conn_id=AWS_CONN_ID,
    time_zone=local_tz,
)


aws_emr_etl_operator = AWSEMROperator(
    task_id="create_EMR_cluster_and_execute_ETL",
    dag=dag,
    conn_id=AWS_CONN_ID,
    redshift_conn_id=AWS_REDSHIFT_CONN_ID,
    time_zone=local_tz,
    cluster_identifier=f"news-nlp-emr-{datetime.now(local_tz).strftime('%Y-%m-%d-%H-%M')}",
)
emr_etl_sensor = EmrJobFlowSensor(
    task_id="sense_emr_etl",
    dag=dag,
    job_flow_id="{{ task_instance.xcom_pull('create_EMR_cluster_and_execute_ETL', key='return_value') }}",
    aws_conn_id=AWS_CONN_ID,
)


create_redshift_cluster = AWSRedshiftOperator(
    task_id="create_redshift_cluster",
    dag=dag,
    conn_id=AWS_CONN_ID,
    redshift_conn_id=AWS_REDSHIFT_CONN_ID,
    time_zone=local_tz,
    # cluster_identifier=f"news-nlp-redshift-{datetime.now(local_tz).strftime('%Y-%m-%d-%H-%M')}",
    cluster_identifier="news-nlp-redshift-2020-08-01-15-40",
)
redshift_ready_sensor = AwsRedshiftClusterSensor(
    task_id="sense_redshift_cluster",
    dag=dag,
    cluster_identifier="{{ task_instance.xcom_pull('create_redshift_cluster', key='return_value')[0] }}",
    target_status='available',
    aws_conn_id=AWS_CONN_ID,
)

tables = ['dim_date', 'dim_title', 'dim_ner', 'fact_news']
table_load_ops = [
    S3ToRedshiftTransfer(
        task_id=f"upload_{table}_to_redshift",
        dag=dag,
        redshift_conn_id=AWS_REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CONN_ID,
        schema=os.environ.get('AWS_REDSHIFT_SCHEMA'),
        table=table,
        s3_bucket=os.environ.get('AWS_S3_BUCKET'),
        s3_key=f'{table}.csv',
        copy_options=["CSV QUOTE '`'", 'IGNOREHEADER 1']
    ) for table in tables
]

start_operator >> aws_s3_upload_scripts
aws_s3_upload_scripts >> aws_emr_etl_operator
aws_emr_etl_operator >> emr_etl_sensor

emr_etl_sensor >> create_redshift_cluster
create_redshift_cluster >> redshift_ready_sensor

for table_load_op in table_load_ops:
    redshift_ready_sensor >> table_load_op

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

