from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')



default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution', 
                               dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    s3_bucket ='udacity-dend',
    s3_key = 'log_data',
    query = SqlQueries.copy_query,
    table = 'staging_events',
    json= 's3://udacity-dend/log_json_path.json'
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_bucket = 'udacity-dend',
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    s3_key = 'song_data',
    table = 'staging_songs',
    query = SqlQueries.copy_query   
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table ='songplays',
    query = SqlQueries.songplay_table_insert,
    staging_events_table = 'staging_events',
    staging_songs_table = 'staging_songs',
    page='NextSong'
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    query = SqlQueries.user_table_insert,
    insert_mode = 'empty_table'
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    query = SqlQueries.song_table_insert,
    insert_mode = 'empty_table'
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift', 
    table = 'artists',
    query = SqlQueries.artist_table_insert,
    insert_mode = 'empty_table'
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'times',
    query = SqlQueries.time_table_insert,
    insert_mode = 'empty_table'
)

check_dic = {
    'songplays':'playid',
    'songs':'songid',
    'artists':'artistid',
    'users':'userid',
    'times':'ts'}
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    list_table = check_dic
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >>  load_user_dimension_table
load_songplays_table >>  load_song_dimension_table
load_songplays_table >>  load_artist_dimension_table
load_songplays_table >>  load_time_dimension_table
load_time_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_user_dimension_table  >> run_quality_checks
run_quality_checks >> end_operator