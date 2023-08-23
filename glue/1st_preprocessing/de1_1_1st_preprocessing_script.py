import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import json
import boto3
import requests
from typing import List
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, FloatType
from pyspark.sql.functions import udf, regexp_replace, explode, col, to_date, concat, row_number, lit
from pyspark.sql.window import Window

from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError


class S3Uploader:
    def __init__(self, bucket_name, access_key, secret_key, region_name):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name
        )

    def get_upload_file_path(self):
        """
        1차 전처리된 최종 parquet 파일 및 parquet crc 파일을 s3에 업로드할 경로를 리턴합니다.
        """
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)

        return f's3://{self.bucket_name}/1st_cleaned_data/year={year}/month={month}/day={day}'
    
    def delete_crc_files(self):
        """
        s3에 올라간 parquet crc 파일을 제거합니다.
        """
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)

        remote_path = f'1st_cleaned_data/year={year}/month={month}/day={day}'

        response = self.s3.list_objects(Bucket=self.bucket_name, Prefix=remote_path)

        for obj in response.get('Contents', []):
            file_key = obj['Key']
            if file_key.endswith('.crc'):
                try:
                    self.s3.delete_object(Bucket=self.bucket_name, Key=file_key)
                except NoCredentialsError:
                    raise Exception("AWS credentials not available")


# Function to create DynamicFrame from the catalog
def create_dyf_from_catalog(database_name, table_name):
    today = date.today()
    year = str(today.year)
    month = str(today.month).zfill(2)
    day = str(today.day).zfill(2)
    return glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        push_down_predicate=f'(year=="{year}" and month=="{month}" and day=="{day}")'
    )


def unnest_and_rename(df):
    # Unnest the 'results' column
    df_unnested = df.select("*", explode(df.results).alias("unnested_results")).drop("results")
    
    # Selecting and naming nested fields
    df_final = df_unnested.select(
        "unnested_results.job_id",
        "unnested_results.platform",
        "unnested_results.category",
        "unnested_results.url",
        "unnested_results.company",
        "unnested_results.title",
        "unnested_results.primary_responsibility",
        "unnested_results.required",
        "unnested_results.preferred",
        "unnested_results.end_at",
        "unnested_results.skills",
        "unnested_results.location",
        "unnested_results.welfare",
        "unnested_results.body",
        "unnested_results.company_description",
        "unnested_results.coordinate",
        "year",
        "month",
        "day"
    )
    
    return df_final


def rearrange_dataframe_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        return df.select(*columns)


def process_text_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    for col in columns:
        df = df.withColumn(col, regexp_replace(df[col], "\n", " "))
    return df


def get_secret():
    """
    AWS Secrets Manager를 이용해 환경변수를 불러옵니다.
    """
    secret_name = "prod/de-1-1/back-end"
    REGION_NAME = "ap-northeast-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=REGION_NAME
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret)

    BUCKET_NAME = secret_dict['BUCKET_NAME']
    ACCESS_KEY = secret_dict['AWS_ACCESS_KEY_ID']
    SECRET_KEY = secret_dict['AWS_SECRET_ACCESS_KEY']
    KAKAO_API_TOKEN = secret_dict['KAKAO_API_TOKEN']

    return BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME, KAKAO_API_TOKEN


BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME, KAKAO_API_TOKEN = get_secret()


@udf(returnType=ArrayType(FloatType()))
def get_coordinate_from_location(location, KAKAO_API_TOKEN=KAKAO_API_TOKEN):
    headers = {'Authorization': 'KakaoAK ' + KAKAO_API_TOKEN}
    
    """
    카카오 API를 통해 location으로부터 coordinate(lat, lon) 리스트를 반환합니다.
    """
    if location is None:
        return None

    url = f'https://dapi.kakao.com/v2/local/search/address.json?query={location}'

    try:
        response = requests.get(url, headers=headers, timeout=5)
        result = json.loads(response.text)
        match_first = result['documents'][0]['address']
        return [float(match_first['y']), float(match_first['x'])]

    except (requests.exceptions.RequestException, TypeError, ValueError, KeyError, IndexError) as e:
        print(f'Error occurred: {e} while fetching address: {location}')


# # 플랫폼별 중복제거 함수
# def get_recent_jd_data(df):
#     df = df.withColumn('date', to_date(concat(col('year'), lit('-'), col('month'), lit('-'), col('day')), "yyyy-MM-dd"))
#     window_spec = Window.partitionBy('job_id', 'category').orderBy(col('date').desc())
#     sorted_df = df.withColumn('row_number', row_number().over(window_spec)) \
#                   .filter(col('row_number') == 1) \
#                   .drop('row_number', 'date')
    
#     return sorted_df


# Initialize the Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = (glueContext.spark_session.builder
         .appName("Python Spark preprocessing #1")
         .master("local")
         .getOrCreate())
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") #success 메타 파일
job = Job(glueContext)
database_name = 'de1_1_database'

# Create DynamicFrames using the function
jobplanet_dyf = create_dyf_from_catalog(database_name, "jobplanet")
rallit_dyf = create_dyf_from_catalog(database_name, "rallit")
jumpit_dyf = create_dyf_from_catalog(database_name, "jumpit")
wanted_dyf = create_dyf_from_catalog(database_name, "wanted")

# Convert DynamicFrames to Spark DataFrames
jobplanet_df = jobplanet_dyf.toDF()
rallit_df = rallit_dyf.toDF()
jumpit_df = jumpit_dyf.toDF()
wanted_df = wanted_dyf.toDF()

# Apply the refactored function
# jobplanet_df = unnest_and_rename(jobplanet_df)

# # 여기서 플랫폼별 중복제거
# # year, month, day 기준으로 정렬 -> 'job_id' + 'category' 기준으로 date가 최신꺼만 남기고 다 삭제
# rallit_not_duplicate_df = get_recent_jd_data(rallit_df)
# wanted_not_duplicate_df = get_recent_jd_data(wanted_df)
# jumpit_not_duplicate_df = get_recent_jd_data(jumpit_df)
# jobplanet_not_duplicate_df = get_recent_jd_data(jobplanet_df)

# # 스키마를 rallit_df 스키마와 동일하게 조정 (중복제거 했을 때)
# rallit_df = rallit_not_duplicate_df
# wanted_df = wanted_not_duplicate_df.select(rallit_df.columns)
# jumpit_df = jumpit_not_duplicate_df.select(rallit_df.columns)
# jobplanet_df = jobplanet_not_duplicate_df.select(rallit_df.columns)

# 스키마를 rallit_df 스키마와 동일하게 조정
wanted_df = wanted_df.select(rallit_df.columns)
jumpit_df = jumpit_df.select(rallit_df.columns)
jobplanet_df = jobplanet_df.select(rallit_df.columns)

# Union the DataFrames
df_final = rallit_df.union(wanted_df).union(jobplanet_df).union(jumpit_df)

# 대분류, 중분류, 카테고리를 나타내는 데이터
categories = [
    ("WEB", "서버/백엔드 개발자", ["서버 개발자", "자바 개발자", "Node.js 개발자", "PHP 개발자", "웹 개발자", "루비온레일즈 개발자", ".NET 개발자", "백엔드 개발", "웹개발", "BACKEND_DEVELOPER", "서버/백엔드 개발자", "웹 풀스택 개발자"]),
    ("WEB", "프론트엔드 개발자", ["프론트엔드 개발자","프론트엔드 개발","FRONTEND_DEVELOPER"]),
    ("WEB", "웹 퍼블리셔", ["웹 퍼블리셔","웹퍼블리셔"]),
    ("GAME", "게임 개발자", ["게임개발", "게임 클라이언트 개발자", "게임 서버 개발자"]),
    ("GAME", "VR/AR/3D", ["VR 엔지니어", "그래픽스 엔지니어", "VR/AR/3D,게임 클라이언트 개발자"]),
    ("DATA", "데이터 사이언티스트", ["데이터 사이언티스트", "DATA_SCIENTIST"]),
    ("DATA", "데이터 엔지니어", ["데이터 엔지니어", "빅데이터 엔지니어", "DATA_ENGINEER"]),
    ("DATA", "데이터 분석가", ["BI 엔지니어", "데이터 분석가", "DATA_ANALYST"]),
    ("DATA", "AI 엔지니어", ["머신러닝 엔지니어", "영상,음성 엔지니어", "MACHINE_LEARNING", "인공지능/머신러닝"]),
    ("DATA", "DBA", ["DBA", "빅데이터 엔지니어,DBA"]),
    ("MOBILE", "안드로이드 개발자", ["안드로이드 개발자", "안드로이드 개발", "ANDROID_DEVELOPER"]),
    ("MOBILE", "iOS 개발자", ["iOS 개발자", "iOS", "IOS_DEVELOPER", "IOS 개발자"]),
    ("MOBILE", "크로스 플랫폼 모바일 개발자", ["크로스플랫폼 앱 개발자", "크로스플랫폼 앱개발자", "CROSS_PLATFORM_DEVELOPER"]),
    ("SUPPORT", "PM", ["개발 매니저", "프로덕트 매니저", "AGILE_SCRUM_MASTER", "인공지능/머신러닝,개발 PM"]),
    ("SUPPORT", "QA 엔지니어", ["QA,테스트 엔지니어", "QA", "QA_ENGINEER", "QA 엔지니어"]),
    ("SUPPORT", "기술지원", ["기술지원", "SUPPORT_ENGINEER"]),
    ("DEVSECOPS", "데브옵스/인프라 엔지니어", ["DevOps / 시스템 관리자", "시스템,네트워크 관리자", "네트워크/보안/운영", "클라우드 개발", "DEV_OPS", "INFRA_ENGINEER", "devops/시스템 엔지니어"]),
    ("DEVSECOPS", "정보보안 담당자", ["보안 엔지니어", "CIO,Chief Information Officer", "SECURITY_ENGINEER", "정보보안 담당자"]),
    ("SW/HW/IOT", "HW/임베디드 개발자", ["임베디드 개발자", "하드웨어 엔지니어", "하드웨어 개발", "HARDWARE_EMBEDDED_ENGINEER", "HW/임베디드"]),
    ("SW/HW/IOT", "소프트웨어 개발자", ["소프트웨어 엔지니어", "파이썬 개발자", "C,C++ 개발자", "소프트웨어 개발", "소프트웨어아키텍트", "SOFTWARE_ENGINEER", "SW/솔루션"]),
    ("ETC", "블록체인 엔지니어", ["블록체인 플랫폼 엔지니어", "BLOCKCHAIN_ENGINEER", "프론트엔드 개발자,블록체인"]),
    ("ETC", "기타", ["ERP전문가", "CTO,Chief Technology Officer", "CTO", "ERP", "etc"])
]

data_list = []
for major_category, middle_category, job_list in categories:
    for sub_category in job_list:
        data_list.append((major_category, middle_category, sub_category))

schema = StructType([
    StructField("major_category", StringType(), True),
    StructField("middle_category", StringType(), True),
    StructField("sub_category", StringType(), True)
])
mapping_df = spark.createDataFrame(data_list, schema=schema)
df_with_mapped_categories = df_final.join(mapping_df, df_final.category == mapping_df.sub_category, "left")

new_columns = [
        'job_id','platform', 'category', 'major_category', 'middle_category', \
        'sub_category', 'company', 'title', 'preferred', 'required', 'primary_responsibility', \
        'url', 'end_at', 'skills', 'location', 'welfare', 'body', 'company_description', 'coordinate'
    ]

df_with_mapped_categories = rearrange_dataframe_columns(df_with_mapped_categories, new_columns)
df_with_mapped_categories = rearrange_dataframe_columns(df_with_mapped_categories, new_columns)

text_columns = ['preferred', 'required', 'primary_responsibility', 'welfare', 'company_description']

df_final = process_text_columns(df_with_mapped_categories, text_columns)
df_filter_for_wanted_rallit = df_final.filter((df_final['platform'] == 'wanted') | (df_final['platform'] == 'rallit'))
df_filter_for_jobplanet_jumpit = df_final.filter((df_final['platform'] == 'jobplanet') | (df_final['platform'] == 'jumpit'))

df_with_coordinate = df_filter_for_jobplanet_jumpit.withColumn('coordinate', get_coordinate_from_location('location'))

result_df = df_filter_for_wanted_rallit.union(df_with_coordinate)

result_repartitioned_df = result_df.repartition(1)

uploader = S3Uploader(BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME)
upload_file_path = uploader.get_upload_file_path()
result_repartitioned_df.write.parquet(upload_file_path, mode="overwrite")
uploader.delete_crc_files()

# glueContext.stop() -> Glue는 job.commit으로 stop 역할을 합니다.
job.commit()
sc.stop()

job.commit()
