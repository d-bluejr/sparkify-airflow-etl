from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

redshift_id="redshift"
aws_id="aws_credentials"

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_events,
    create_params=SqlQueries.staging_events_table_create,
    s3_path='s3://udacity-dend/log_data',
    is_json=True,
    json_config='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_songs,
    create_params=SqlQueries.staging_songs_table_create,
    s3_path='s3://udacity-dend/song_data',
    is_json=True,
    json_config='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.songplays,
    dest_table_create_params=SqlQueries.songplay_table_create,
    dest_table_insert_values=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.users,
    dest_table_create_params=SqlQueries.user_table_create,
    dest_table_insert_values=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.songs,
    dest_table_create_params=SqlQueries.song_table_create,
    dest_table_insert_values=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.artists,
    dest_table_create_params=SqlQueries.artist_table_create,
    dest_table_insert_values=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.time,
    dest_table_create_params=SqlQueries.time_table_create,
    dest_table_insert_values=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift_id
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator