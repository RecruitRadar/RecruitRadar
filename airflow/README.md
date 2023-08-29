## Airflow DAGs Graph

### 1. {platform_name}_job_api_scraper_dag
![dags_jobplanet_job_api_scraper_dag_graph](https://github.com/RecruitRadar/RecruitRadar/assets/74031620/f62c8639-e941-44db-b598-117d0cb50faa)

예시로 jobplanet_job_api_scraper_dag 그래프를 가져왔습니다. jobplanet 자리에 각 플랫폼명이 들어갑니다. 각 플랫폼의 API Scraper를 trigger 합니다.

### 2. glue_crawler_dag (1)
![dags_glue_crawler_dag_graph](https://github.com/RecruitRadar/RecruitRadar/assets/74031620/e1a053af-a352-47be-a4ad-91d2abc242ac)

각 플랫폼별 AWS Glue crawler를 trigger 합니다.

### 3. glue_etl_job_dag
![dags_glue_etl_job_dag_graph](https://github.com/RecruitRadar/RecruitRadar/assets/74031620/8421ca6c-496d-4d84-be9f-892bc713c908)

1차 전처리 Glue job을 trigger 합니다.

### 4. glue_crawler_dag (2)
1차 전처리 결과에 대해 Glue crawler를 trigger 합니다.

### 5. glue_nlp_job_dag
![dags_glue_nlp_job_dag_graph](https://github.com/RecruitRadar/RecruitRadar/assets/74031620/0ce88526-61e0-4efb-96a8-af3234097899)

2차 전처리 Glue job을 trigger 합니다.

### 6. glue_crawler_dag (3)
2차 전처리 결과에 대해 Glue crawler를 trigger 합니다.

### 7. athena_query_dag
![dags_athena_query_dag_graph](https://github.com/RecruitRadar/RecruitRadar/assets/74031620/d0a40337-919e-48b7-8a2c-b41594ed53cf)

Amazon Athena 쿼리들을 trigger 합니다. 여기서 Array의 경우 unnesting을 진행합니다.

### 8. redshift_elt_query_dag
![dags_redshift_elt_query_dag_graph](https://github.com/RecruitRadar/RecruitRadar/assets/74031620/2f04d302-8d91-457b-8ec6-58f370bef890)

마지막으로 Amazon Redshift Spectrum ELT 쿼리를 trigger합니다. 데이터 마트 테이블을 생성합니다.
