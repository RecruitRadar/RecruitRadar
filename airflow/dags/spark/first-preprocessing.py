import os
import json
import boto3
import glob
import requests
from typing import List
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, FloatType
from pyspark.sql.functions import udf
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError


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


class S3Downloader:
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
    
    def get_file_path(self, platform_name: str) -> str:
        """
        s3_key가 될 remote_path를 생성합니다.
        """
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)

        return f'{platform_name}/year={year}/month={month}/day={day}/{platform_name}.json'
    
    def download_file(self, remote_path, local_path):
        try:
            self.s3.download_file(self.bucket_name, remote_path, local_path)
        except NoCredentialsError:
            raise Exception("AWS credentials not available")


class Preprocessing:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Python Spark preprocessing #1").master("local").getOrCreate()

    def read_json_files(self, file_paths: List[str]) -> DataFrame:
        data = []
        for path in file_paths:
            with open(path) as json_file:
                json_data = json.load(json_file)
                if "results" in json_data:
                    data.append(path)
        
        # 모든 JSON 파일을 한 번에 읽어 DataFrame 생성
        df = self.spark.read.option("multiline", "true").json(data)
        
        df_final = df.selectExpr("explode(results) as results")

        df_final = df_final.select(
            "results.job_id",
            "results.platform",
            "results.category",
            "results.url",
            "results.company",
            "results.title",
            "results.primary_responsibility",
            "results.required",
            "results.preferred",
            "results.end_at",
            "results.skills",
            "results.location",
            "results.welfare",
            "results.body",
            "results.company_description",
            "results.coordinate"
        )
        
        return df_final

    def process_text_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        for col in columns:
            df = df.withColumn(col, regexp_replace(df[col], "\n", " "))
        return df

    def stop_spark_session(self):
        self.spark.stop()


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

    def upload_file(self):
        """
        1차 전처리된 최종 parquet 파일 및 parquet crc 파일을 s3에 업로드합니다.
        """
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)

        remote_path = f'1st_cleaned_data/year={year}/month={month}/day={day}'

        folder_path = f'data/1st_cleaned_data'

        for root, _, files in os.walk(folder_path):
            for file in files:
                local_path = os.path.join(root, file)
                result_remote_path = f"{remote_path}/{file}"
                
                try:
                    self.s3.upload_file(local_path, self.bucket_name, result_remote_path)
                except NoCredentialsError:
                    raise Exception("AWS credentials not available")
    
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


def main():
    headers = {'Authorization': 'KakaoAK ' + KAKAO_API_TOKEN}

    @udf(returnType=ArrayType(FloatType()))
    def get_coordinate_from_location(location):
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

    
    downloader = S3Downloader(BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME)

    os.makedirs('data', exist_ok=True)

    platform_list = ['jobplanet', 'jumpit', 'rallit', 'wanted']
    for platform in platform_list:
        json_file_path = downloader.get_file_path(platform)
        downloader.download_file(json_file_path, f'data/{platform}.json')
    
    spark_preprocessor = Preprocessing()
    spark_preprocessor.spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    file_paths = glob.glob(f'{os.getcwd()}/data/*.json')

    if not file_paths:
        raise ValueError("No JSON files found in the specified directory!")

    df_final = spark_preprocessor.read_json_files(file_paths)
    
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
    mapping_df = spark_preprocessor.spark.createDataFrame(data_list, schema=schema)

    df_with_mapped_categories = df_final.join(mapping_df, df_final.category == mapping_df.sub_category, "left")

    new_columns = [
        'job_id','platform', 'category', 'major_category', 'middle_category', \
        'sub_category', 'company', 'title', 'preferred', 'required', 'primary_responsibility', \
        'url', 'end_at', 'skills', 'location', 'welfare', 'body', 'company_description', 'coordinate'
    ]

    def rearrange_dataframe_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        return df.select(*columns)

    df_with_mapped_categories = rearrange_dataframe_columns(df_with_mapped_categories, new_columns)

    text_columns = ['preferred', 'required', 'primary_responsibility', 'welfare', 'company_description']
    df_final = spark_preprocessor.process_text_columns(df_with_mapped_categories, text_columns)

    df_filter_for_wanted_rallit = df_final.filter((df_final['platform'] == 'wanted') | (df_final['platform'] == 'rallit'))
    df_filter_for_jobplanet_jumpit = df_final.filter((df_final['platform'] == 'jobplanet') | (df_final['platform'] == 'jumpit'))

    df_with_coordinate = df_filter_for_jobplanet_jumpit.withColumn('coordinate', get_coordinate_from_location('location'))

    result_df = df_filter_for_wanted_rallit.union(df_with_coordinate)

    result_repartitioned_df = result_df.repartition(1)

    output_path = "data/1st_cleaned_data"
    result_repartitioned_df.write.parquet(output_path, mode="overwrite")

    spark_preprocessor.stop_spark_session()

    uploader = S3Uploader(BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME)
    uploader.upload_file()
    uploader.delete_crc_files()


if __name__ == "__main__":
    main()
