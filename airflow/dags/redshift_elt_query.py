from datetime import datetime, timedelta
from airflow import DAG
from aws_redshift_operator import RedshiftOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
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
    'redshift_elt_query_dag',
    default_args=default_args,
    description='DAG for redshift queries to create data mart',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    template_searchpath='/opt/airflow/dags/sqls/redshift',
    catchup=False
)

WORK_GROUP_NAME = 'de-1-1-redshift'
AWS_CONN_ID = 'aws_default'
DATABASE = 'dev'
SCHEMA = 'analytics'


start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)

def short_delay():
    time.sleep(10)

initialize_schema_task = RedshiftOperator(
            task_id='run_redshift_initialize_schema',
            sql='initialize_external_schema.sql',
            database=DATABASE,
            work_group_name=WORK_GROUP_NAME,
            aws_conn_id=AWS_CONN_ID,
            dag=dag
        )

create_schema_task = RedshiftOperator(
            task_id='run_redshift_create_schema',
            sql='create_schema.sql',
            database=DATABASE,
            work_group_name=WORK_GROUP_NAME,
            aws_conn_id=AWS_CONN_ID,
            dag=dag
        )

drop_unique_jds_task = RedshiftOperator(
            task_id=f'run_redshift_drop_unique_jds',
            sql=f'DROP TABLE IF EXISTS {SCHEMA}.unique_jds',
            database=DATABASE,
            work_group_name=WORK_GROUP_NAME,
            aws_conn_id=AWS_CONN_ID,
            dag=dag
        )

delay_unique_jds_task = PythonOperator(
    task_id=f'short_delay_before_create_unique_jds',
    python_callable=short_delay,
    dag=dag
)

create_unique_jds_task = RedshiftOperator(
    task_id=f'run_redshift_create_unique_jds',
    sql=f'create_unique_jds.sql',
    database=DATABASE,
    work_group_name=WORK_GROUP_NAME,
    aws_conn_id=AWS_CONN_ID,
    dag=dag
)

elt_table_list = [
    'unique_category_count_sub',
    'unique_company_coordinates',
    'unique_welfare_korean',
    'unique_welfare_english',
    'unique_jd_skills',
    'unique_preferred_english',
    'unique_primary_responsibility_english',
    'unique_required_english',
    'unique_category_count_major',
    'unique_category_count_major_middle',
    'unique_upcoming_deadline_jobs_7days',
    'unique_daily_job_posting_count',
] # set the elt table list

dimension_drop_tasks = []
dimension_create_tasks = []

def process_elt_table_queries(table_list):
    for table_name in table_list:
        drop_query = f'DROP TABLE IF EXISTS {SCHEMA}.{table_name}'
        drop_task = RedshiftOperator(
            task_id=f'run_redshift_drop_{table_name}',
            sql=drop_query,
            database=DATABASE,
            work_group_name=WORK_GROUP_NAME,
            aws_conn_id=AWS_CONN_ID,
            dag=dag
        )

        delay_task = PythonOperator(
            task_id=f'short_delay_before_create_{table_name}',
            python_callable=short_delay,
            dag=dag
        )

        create_task = RedshiftOperator(
            task_id=f'run_redshift_create_{table_name}',
            sql=f'create_{table_name}.sql',
            database=DATABASE,
            work_group_name=WORK_GROUP_NAME,
            aws_conn_id=AWS_CONN_ID,
            dag=dag
        )


        drop_task >> delay_task >> create_task
        dimension_drop_tasks.append(drop_task)
        dimension_create_tasks.append(create_task)

process_elt_table_queries(elt_table_list)

# Setting up the dependencies for start and end tasks
start_task >> initialize_schema_task >> create_schema_task >> drop_unique_jds_task >> delay_unique_jds_task >> create_unique_jds_task >> dimension_drop_tasks
dimension_create_tasks >> end_task



