from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StringType, StructField, StructType
import os
import json
import boto3
import glob
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

class S3Downloader:
    def __init__(self, bucket_name, access_key, secret_key):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

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
                # 키 이름 변경
                if "result" in json_data:
                    data.append(path)
        
        # 모든 JSON 파일을 한 번에 읽어 DataFrame 생성
        df = self.spark.read.option("multiline", "true").json(data)
        
        df_final = df.selectExpr("explode(result) as result")

        df_final = df_final.select(
            "result.job_id",
            "result.platform",
            "result.category",
            "result.url",
            "result.company",
            "result.title",
            "result.primary_responsibility",
            "result.required",
            "result.preferred",
            "result.end_at",
            "result.skills",
            "result.location",
            "result.welfare",
            "result.body",
            "result.company_description",
            "result.coordinate"
        )
        
        return df_final

    def process_text_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        for col in columns:
            df = df.withColumn(col, regexp_replace(df[col], "\n", " "))
        return df

    def stop_spark_session(self):
        self.spark.stop()

        
def main():
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

    downloader = S3Downloader(BUCKET_NAME, ACCESS_KEY, SECRET_KEY)

    os.makedirs('data', exist_ok=True)
    downloader.download_file('jobplanet/year=2023/month=08/day=14/jobplanet.json', 'data/jobplanet.json')
    downloader.download_file('jumpit/year=2023/month=08/day=12/jumpit.json', 'data/jumpit.json')
    downloader.download_file('rallit/year=2023/month=08/day=12/rallit.json', 'data/rallit.json')
    downloader.download_file('wanted/year=2023/month=08/day=12/wanted.json', 'data/wanted.json')

    spark_preprocessor = Preprocessing()

    file_paths = glob.glob(f'{os.getcwd()}/data/*.json')

    if not file_paths:
        raise ValueError("No JSON files found in the specified directory!")

    df_final = spark_preprocessor.read_json_files(file_paths)

    # 결과 확인
    df_final.show()

    data_list = []
    # 대분류, 중분류, 카테고리를 나타내는 데이터
    categories = [
    ("WEB", "서버/백엔드 개발자", ["서버 개발자", "자바 개발자", "Node.js 개발자", "PHP 개발자", "웹 개발자", "루비온레일즈 개발자", ".NET 개발자"]),
    ("WEB", "프론트엔드 개발자", ["프론트엔드 개발자","프론트엔드 개발","FRONTEND_DEVELOPER"]),
    ("WEB", "웹 퍼블리셔", ["웹 퍼블리셔","웹퍼블리셔"]),
    ("GAME", "게임 개발자", ["게임개발", "게임 클라이언트 개발자", "게임 서버 개발자"]),
    ("GAME", "VR/AR/3D", ["VR 엔지니어", "그래픽스 엔지니어", "VR/AR/3D,게임 클라이언트 개발자"]),
    ("DATA", "데이터 사이언티스트", ["데이터 사이언티스트", "DATA_SCIENTIST"]),
    ("DATA", "데이터 엔지니어", ["데이터 엔지니어", "빅데이터 엔지니어", "DATA_ENGINEER"]),
    ("DATA", "데이터 분석가", ["BI 엔지니어", "데이터 분석가", "DATA_ANALYST"]),
    ("DATA", "AI 엔지니어", ["머신러닝 엔지니어", "영상,음성 엔지니어", "MACHINE_LEARNING", "인공지능/머신러닝"]),
    ("DATA", "DBA", ["DBA", "빅데이터 엔지니어,DBA"]),
    ("MOBILE", "안드로이드 개발자", ["안드로이드 개발자", "ANDROID_DEVELOPER"]),
    ("MOBILE", "iOS 개발자", ["iOS 개발자", "IOS", "IOS_DEVELOPER"]),
    ("MOBILE", "크로스 플랫폼 모바일 개발자", ["크로스플랫폼 앱 개발자", "CROSS_PLATFORM_DEVELOPER"]),
    ("SUPPORT", "PM", ["개발 매니저", "프로덕트 매니저", "AGILE_SCRUM_MASTER"]),
    ("SUPPORT", "QA 엔지니어", ["QA,테스트 엔지니어", "QA", "QA_ENGINEER"]),
    ("SUPPORT", "기술지원", ["기술지원", "SUPPORT_ENGINEER"]),
    ("DEVSECOPS", "데브옵스/인프라 엔지니어", ["DevOps / 시스템 관리자", "시스템,네트워크 관리자", "DEV_OPS", "INFRA_ENGINEER", "devops/시스템 엔지니어"]),
    ("DEVSECOPS", "정보보안 담당자", ["보안 엔지니어", "CIO,Chief Information Officer", "SECURITY_ENGINEER"]),
    ("SW/HW/IOT", "HW/임베디드 개발자", ["임베디드 개발자", "하드웨어 엔지니어", "HARDWARE_EMBEDDED_ENGINEER"]),
    ("SW/HW/IOT", "소프트웨어 개발자", ["소프트웨어 엔지니어", "파이썬 개발자", "C,C++ 개발자", "SOFTWARE_ENGINEER"]),
    ("ETC", "블록체인 엔지니어", ["블록체인 플랫폼 엔지니어", "BLOCKCHAIN_ENGINEER"]),
    ("ETC", "기타", ["ERP전문가", "CTO,Chief Technology Officer", "ERP"])
    ]

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

    new_columns = ['job_id', 'platform', 'category', 'major_category', 'middle_category', 'sub_category', 'company', 'title', 'preferred', 'required', 'primary_responsibility', 'url', 'end_at', 'skills', 'location', 'welfare', 'body', 'company_description', 'coordinate']

    def rearrange_dataframe_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        return df.select(*columns)

    df_with_mapped_categories = rearrange_dataframe_columns(df_with_mapped_categories, new_columns)

    text_columns = ['preferred', 'required', 'primary_responsibility', 'welfare', 'company_description']
    df_final = spark_preprocessor.process_text_columns(df_with_mapped_categories, text_columns)

    output_path = "data/cleaned_data.parquet"
    df_final.write.parquet(output_path, mode="overwrite")

    spark_preprocessor.stop_spark_session()
    
    print('finish')

if __name__ == "__main__":
    main()