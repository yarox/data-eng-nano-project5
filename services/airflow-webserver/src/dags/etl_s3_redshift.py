from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG

from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)

from helpers import SqlQueries as Q


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'catchup': False,
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_s3_redshift',
    default_args=default_args,
    description='Load and transform S3 data into Redshift',
    schedule_interval='@hourly',
)

start_task = DummyOperator(task_id='begin_execution', dag=dag)
end_task = DummyOperator(task_id='stop_execution', dag=dag)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    conn_id='redshift',
    cases=[
        {
            'query': 'SELECT COUNT(*) FROM users WHERE user_id is NULL',
            'expected_result': 0,
        },
        {
            'query': 'SELECT COUNT(*) FROM songs WHERE song_id is NULL',
            'expected_result': 0,
        },
    ],
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events_to_redshift',
    dag=dag,
    conn_id='redshift',
    credentials_id='aws_credentials',
    table_name='staging_events',
    table_create=Q.staging_events_table_create,
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    json_option='s3://udacity-dend/log_json_path.json',
    drop_table=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs_to_redshift',
    dag=dag,
    conn_id='redshift',
    credentials_id='aws_credentials',
    table_name='staging_songs',
    table_create=Q.staging_songs_table_create,
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    json_option='auto',
    drop_table=True,
)

load_songplays_fact_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    conn_id='redshift',
    table_name='songplays',
    table_create=Q.songplays_table_create,
    table_select=Q.songplays_table_select,
    drop_table=True,
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='load_users_dimension_table',
    dag=dag,
    conn_id='redshift',
    table_name='users',
    table_create=Q.users_table_create,
    table_select=Q.users_table_select,
    drop_table=True,
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='load_songs_dimension_table',
    dag=dag,
    conn_id='redshift',
    table_name='songs',
    table_create=Q.songs_table_create,
    table_select=Q.songs_table_select,
    drop_table=True,
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='load_artists_dimension_table',
    dag=dag,
    conn_id='redshift',
    table_name='artists',
    table_create=Q.artists_table_create,
    table_select=Q.artists_table_select,
    drop_table=True,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dimension_table',
    dag=dag,
    conn_id='redshift',
    table_name='time',
    table_create=Q.time_table_create,
    table_select=Q.time_table_select,
    drop_table=True,
)

stage_tasks = [stage_events_to_redshift, stage_songs_to_redshift]
load_dimension_table_tasks = [
    load_users_dimension_table,
    load_songs_dimension_table,
    load_artists_dimension_table,
    load_time_dimension_table,
]

tasks = (
    start_task
    >> stage_tasks
    >> load_songplays_fact_table
    >> load_dimension_table_tasks
    >> run_quality_checks
    >> end_task
)
