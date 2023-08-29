<div align='center'>
<img src="https://github.com/RecruitRadar/RecruitRadar/assets/74031620/b27943bb-19a0-49e9-a5d6-831a9a143f50" width='150' >


# RecruitRadar

채용 공고 데이터 파이프라인 구축을 통한 개발 스택 트렌드 분석

</div>

## Project Duration
2023.08.04 ~ 2023.09.04

## Team Members

| **김민석** | **서대원** | **유하준** | **정희원** |
|:---:|:---:|:---:|:---:|
| <img src="https://github.com/kmus1232.png" width='400' /> | <img src="https://github.com/DaewonSeo.png" width='400' /> | <img src="https://github.com/HaJunYoo.png" width='400' /> | <img src="https://github.com/heewoneha.png" width='400' /> |

## Project Architecture
![System Architecture](https://github.com/RecruitRadar/RecruitRadar/assets/74031620/e9c654e5-0124-4771-ae62-9102342fc5af)

## ERD

### 1. raw_data_external schema
<img src="https://github.com/RecruitRadar/RecruitRadar/assets/74031620/76f5668a-cb4d-42aa-b4f4-d036c72c4f36" width='800' />

### 2. analytics schema
<img src="https://github.com/RecruitRadar/RecruitRadar/assets/74031620/6f9c731d-63bb-43fa-8ea8-e5b600267465" width='800' />

## Airflow Flowchart
[Click here!](/airflow/README.md)

## Tech Stack

| Field | Stack |
|:---:|:---|
| API Back-end | <img src="https://img.shields.io/badge/Fast API-088A68?style=for-the-badge&logo=fastapi&logoColor=white"/> <img src="https://img.shields.io/badge/AMAZON EC2-FF8000?style=for-the-badge&logo=amazonec2&logoColor=white"/> |
| Data Warehouse | <img src="https://img.shields.io/badge/Amazon Redshift Spectrum-045FB4?style=for-the-badge&logo=amazonredshift&logoColor=white"/> <img src="https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white"/> |
| ETL & ELT |  <img src="https://img.shields.io/badge/AMAZON S3-088A08?style=for-the-badge&logo=amazons3&logoColor=white"/> <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/> <img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white"/> <img src="https://img.shields.io/badge/AWS Glue-7401DF?style=for-the-badge&logo=Amazon+AWS&logoColor=white"/> <img src="https://img.shields.io/badge/Amazon Athena-7401DF?style=for-the-badge&logo=Amazon+AWS&logoColor=white"/> |
| Dashboard | <img src="https://img.shields.io/badge/Preset-04B404?style=for-the-badge&logo=preset&logoColor=white"/> |
| CI/CD | <img src="https://img.shields.io/badge/github actions-181717?style=for-the-badge&logo=githubactions&logoColor=white"> <img src="https://img.shields.io/badge/amazon codedeploy-013ADF?style=for-the-badge&logo=Amazon+AWS&logoColor=white"> |
| ETC | <img src="https://img.shields.io/badge/AWS Secrets Manager-DF0101?style=for-the-badge&logo=Amazon+AWS&logoColor=white"/> <img src="https://img.shields.io/badge/Slack-240B3B?style=for-the-badge&logo=slack&logoColor=white"/> |

## Usage

### Repository Structures

  ```bash
  .
  ├── .github
  │   ├── workflows
  │   │   ├── scraper.yml
  │   │   └── test.yml
  ├── README.md
  ├── airflow
  │   ├── Dockerfile.mac
  │   ├── Dockerfile.ubuntu
  │   ├── README.md
  │   ├── dags
  │   │   ├── sqls
  │   │   │   ├── athena
  │   │   │   │   ├── create_company_detail.sql
  │   │   │   │   ├── create_daily_jd_table.sql
  │   │   │   │   ├── create_jd_preferred_english_nouns.sql
  │   │   │   │   ├── create_jd_preferred_korean_nouns.sql
  │   │   │   │   ├── create_jd_primary_responsibility_english_nouns.sql
  │   │   │   │   ├── create_jd_primary_responsibility_korean_nouns.sql
  │   │   │   │   ├── create_jd_required_english_nouns.sql
  │   │   │   │   ├── create_jd_required_korean_nouns.sql
  │   │   │   │   ├── create_jd_skills.sql
  │   │   │   │   ├── create_jd_welfare_english_nouns.sql
  │   │   │   │   └── create_jd_welfare_korean_nouns.sql
  │   │   │   ├── redshift
  │   │   │   │   ├── create_unique_jds.sql
  │   │   │   │   ├── drop_schema.sql
  │   │   │   │   ├── drop_unique_jds.sql
  │   │   │   │   └── initialize_external_schema.sql
  │   │   ├── athena_query.py
  │   │   ├── glue_crawler.py
  │   │   ├── glue_etl_job.py
  │   │   ├── glue_nlp_job.py
  │   │   ├── jobplanet_api_call.py
  │   │   ├── jumpit_api_call.py
  │   │   ├── rallit_api_call.py
  │   │   ├── redshift_elt_query.py
  │   │   └── wanted_api_call.py
  │   ├── docker-compose.mac.yaml
  │   ├── docker-compose.ubuntu.yaml
  │   ├── plugins
  │   │   ├── aws_athena_operator.py
  │   │   ├── aws_redshift_operator.py
  │   │   └── long_http_operator.py
  │   └──  requirements.txt
  ├── appspec.yml
  ├── eda
  │   ├── Dockerfile
  │   ├── EDA-spark.ipynb
  │   ├── EDA.ipynb
  │   ├── docker-compose.yml
  │   ├── font
  │   │   └── applegothic.ttf
  │   └── requirements.txt
  ├── glue
  │   ├── 1st_preprocessing
  │   │   ├── de1_1_1st_preprocessing_notebook.ipynb
  │   │   └── de1_1_1st_preprocessing_script.py
  │   └── 2nd_preprocessing
  │       └── de1_1_2nd_preprocessing_script.py
  └── scraper_server
      ├── Dockerfile
      ├── api
      │   └── scraper.py
      ├── docker-compose.yml
      ├── main.py
      ├── plugin
      │   ├── jobplanet_class.py
      │   ├── jumpit_class.py
      │   ├── rallit_class.py
      │   └── wanted_class.py
      ├── requirements.txt
      ├── server.py
      └── test_app.py
  ```

### Summary

| Directory | Explanation |
|:---:|:---|
| `.github` | github actions 관련 파일 |
| `airflow/dags` | airflow dag 관련 파일 |
| `airflow/dags/sqls` | athena 및 redshift 쿼리 모음 |
| `eda` | eda 모음 |
| `glue` | 1차 & 2차 전처리 glue job 관련 파일 |
| `scraper_server` | 각종 플랫폼으로부터 데이터를 스크래핑하는 Fast API 파일 모음. `$ python server.py` |
