from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from helpers.sql_queries import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'depends_on_past': False,
    'email_on_retry': False,
    'owner': 'udacity',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 12),
}

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'create_tables.sql')) as f:
    create_tables_sql = f.read()

dag = DAG('udacity_sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_format="'s3://udacity-dend/log_json_path.json'",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_format="'auto'",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id="redshift",
    truncate_table=False,
    select_sql=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    truncate_table=True,
    select_sql=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id="redshift",
    truncate_table=True,
    select_sql=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    truncate_table=True,
    select_sql=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    truncate_table=True,
    select_sql=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['songplays', 'users', 'songs', 'artists', 'time'],
    columns={
        'songplays': ['userid', 'playid'],
        'users': ['first_name', 'last_name'],
        'songs': ['songid', 'title', 'duration'],
        'artists': ['artistid', 'name'],
        'time': ['hour', 'day', 'week', 'month'],
    },
    redshift_conn_id="redshift",
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator \
    >> create_tables \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> end_operator
