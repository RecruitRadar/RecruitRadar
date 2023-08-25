# athena_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from aws_athena_operator import AthenaOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain

import time


default_args = {
    'owner': 'jd_analysis',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'athena_query_dag',
    default_args=default_args,
    description='A simple DAG to run Athena queries',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Constants
DATABASE = 'de1_1_database'
OUTPUT_LOCATION = 's3://de-1-1/athena/'
AWS_CONN_ID = 'aws_default'


# Start and end tasks for better visualization
start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)

def short_delay():
    time.sleep(10)

dimension_drop_tasks = []
dimension_create_tasks = []

def process_table_queries(table_queries):
    for table_name, query_data in table_queries.items():
        drop_query = f'DROP TABLE IF EXISTS {DATABASE}.{table_name};'

        drop_task = AthenaOperator(
            task_id=f'run_athena_drop_{table_name}',
            query=drop_query,
            output_location=OUTPUT_LOCATION,
            database=DATABASE,
            aws_conn_id=AWS_CONN_ID,
            dag=dag
        )

        delay_task = PythonOperator(
            task_id=f'short_delay_before_create_{table_name}',
            python_callable=short_delay,
            dag=dag
        )

        create_query = query_data
            
        create_task = AthenaOperator(
            task_id=f'run_athena_create_{table_name}',
            query=create_query,
            output_location=OUTPUT_LOCATION,
            database=DATABASE,
            aws_conn_id=AWS_CONN_ID,
            dag=dag
        )

        drop_task >> delay_task >> create_task
        
        dimension_drop_tasks.append(drop_task)
        dimension_create_tasks.append(create_task)



# New tables to be processed
new_tables_queries = {
    "daily_jd_table": """
        CREATE TABLE IF NOT EXISTS "de1_1_database"."daily_jd_table" AS
        SELECT DISTINCT
            year,
            month,
            day,
            CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) AS date_str,
            platform,
            job_id,
            company,
            title,
            major_category,
            middle_category,
            sub_category
        FROM
            "de1_1_database"."2nd_processed_data";
    """,
    "company_detail": """
        CREATE TABLE IF NOT EXISTS "de1_1_database"."company_detail" AS
        SELECT DISTINCT
            company,
            CASE WHEN coordinate IS NOT NULL THEN CAST(coordinate[1] AS DOUBLE) ELSE NULL END AS lat,
            CASE WHEN coordinate IS NOT NULL THEN CAST(coordinate[2] AS DOUBLE) ELSE NULL END AS lon,
            company_description
        FROM
            "de1_1_database"."2nd_processed_data";
    """,
    "jd_skills": """
        CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_skills" AS
        SELECT DISTINCT job_id, platform, unnested_skill
        FROM "de1_1_database"."2nd_processed_data", UNNEST(skills) AS t(unnested_skill);
    """,
    
    "jd_preferred_korean_nouns": """
        CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_preferred_korean_nouns" AS
        SELECT DISTINCT job_id, platform, unnested_preferred_korean_nouns
        FROM "de1_1_database"."2nd_processed_data", UNNEST(preferred_korean_nouns) AS t(unnested_preferred_korean_nouns);
    """,
    
    "jd_required_korean_nouns": """
        CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_required_korean_nouns" AS
        SELECT DISTINCT job_id, platform, unnested_required_korean_nouns
        FROM "de1_1_database"."2nd_processed_data", UNNEST(required_korean_nouns) AS t(unnested_required_korean_nouns);
    """,
    
    "jd_primary_responsibility_korean_nouns": """
        CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_primary_responsibility_korean_nouns" AS
        SELECT DISTINCT job_id, platform, unnested_primary_responsibility_korean_nouns
        FROM "de1_1_database"."2nd_processed_data",
        UNNEST(primary_responsibility_korean_nouns) AS t(unnested_primary_responsibility_korean_nouns);
    """,
    
    "jd_welfare_korean_nouns": """
        CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_welfare_korean_nouns" AS
        SELECT DISTINCT job_id, platform, unnested_welfare_korean_nouns
        FROM "de1_1_database"."2nd_processed_data", UNNEST(welfare_korean_nouns) AS t(unnested_welfare_korean_nouns);
    """,
    
    "jd_primary_responsibility_english_nouns": """
        CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_primary_responsibility_english_nouns" AS
        SELECT DISTINCT job_id, platform, unnested_primary_responsibility_english_nouns
        FROM "de1_1_database"."2nd_processed_data",
        UNNEST(primary_responsibility_english_nouns) AS t(unnested_primary_responsibility_english_nouns);
    """,
    
    "jd_welfare_english_nouns":"""
        CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_welfare_english_nouns" AS
        SELECT DISTINCT job_id, platform, unnested_welfare_english_nouns
        FROM "de1_1_database"."2nd_processed_data", 
        UNNEST(welfare_english_nouns) AS t(unnested_welfare_english_nouns);
    """,
    
    "jd_required_english_nouns":"""
        CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_required_english_nouns" AS
        SELECT DISTINCT job_id, platform, unnested_required_english_nouns
        FROM "de1_1_database"."2nd_processed_data", 
        UNNEST(required_english_nouns) AS t(unnested_required_english_nouns);
    """,
    
    "jd_preferred_english_nouns":"""
        CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_preferred_english_nouns" AS
        SELECT DISTINCT job_id, platform, unnested_preferred_english_nouns
        FROM "de1_1_database"."2nd_processed_data",
        UNNEST(preferred_english_nouns) AS t(unnested_preferred_english_nouns);
    """
}

process_table_queries(new_tables_queries)


# Setting up the dependencies for start and end tasks
start_task >> dimension_drop_tasks
dimension_create_tasks >> end_task


