# library for setting glue studio notebook
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# library for conducting 2nd preprocessing
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
from datetime import date
from kiwipiepy import Kiwi
import boto3
import json


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
        1차 전처리된 최종 parquet 파일을 s3에 업로드할 경로를 리턴합니다.
        """
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)

        return f's3://{self.bucket_name}/2nd_processed_data/year={year}/month={month}/day={day}'
        
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


@udf(returnType=ArrayType(StringType()))
def extract_korean_noun(text):
    if text is None or text.strip() == "":
        return []
    kiwi = Kiwi()
    result = kiwi.tokenize(text)
    return [token.form for token in result if token.tag in {'NNG', 'NNP'}]


@udf(returnType=ArrayType(StringType()))
def extract_english_noun(text):
    if text is None or text.strip() == "":
        return []
    kiwi = Kiwi()
    result = kiwi.tokenize(text)
    return [token.form for token in result if token.tag == 'SL']


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
    
dyf = glueContext.create_dynamic_frame.from_catalog(database='de1_1_database', table_name='1st_cleaned_data')
dyf.printSchema()

df = dyf.toDF()
df.show()

drop_cols = ("year", "month", "day")
df = df.drop(*drop_cols)
df.printSchema()
df.show()


df = df.withColumn("preferred_korean_nouns", extract_korean_noun(df["preferred"]))
df = df.withColumn("required_korean_nouns", extract_korean_noun(df["required"]))
df = df.withColumn("primary_responsibility_korean_nouns", extract_korean_noun(df["primary_responsibility"]))
df = df.withColumn("welfare_korean_nouns", extract_korean_noun(df["welfare"]))
df = df.withColumn("preferred_english_nouns", extract_english_noun(df["preferred"]))
df = df.withColumn("required_english_nouns", extract_english_noun(df["required"]))
df = df.withColumn("primary_responsibility_english_nouns", extract_english_noun(df["primary_responsibility"]))
df = df.withColumn("welfare_english_nouns", extract_english_noun(df["welfare"]))
df.show()

repartitioned_df = df.repartition(1)

BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME, _ = get_secret()
uploader = S3Uploader(BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME)
upload_file_path = uploader.get_upload_file_path()

repartitioned_df.write.parquet(upload_file_path, mode="overwrite")

job.commit()
sc.stop()
job.commit()