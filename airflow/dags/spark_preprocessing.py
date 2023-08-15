from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


# DAG default_args 설정
default_args = {
    "owner": "jd_analysis",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

# DAG 생성
dag = DAG(
    "spark-test",
    default_args=default_args,
    schedule_interval= "@once"  # 매일 실행
)

# DAG 태스크 정의
start = DummyOperator(task_id="start", dag=dag)

# SparkSubmitOperator를 사용하여 Spark 작업 실행
spark_app_name = "SparkPreprocessingJob"  # Spark 애플리케이션 이름
spark_master = "spark://spark:7077" # Spark 마스터 URL

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application= "/opt/airflow/dags/spark/first-preprocessing.py" , # Spark 애플리케이션 경로" /home/jovyan/first-preprocessing.py",
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": spark_master},
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

# DAG 태스크 순서 설정
start >> spark_job >> end