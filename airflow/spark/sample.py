from pyspark.sql import SparkSession
from pyspark.sql import Row

# Spark 세션 초기화
spark = SparkSession.builder.appName("DataFrameExample").master("spark://spark:7077").getOrCreate()

# 예제 데이터 리스트 생성
data = [
    Row(name="John", age=28, city="New York"),
    Row(name="Anna", age=22, city="London"),
    Row(name="Mike", age=32, city="San Francisco")
]

# DataFrame 생성
df = spark.createDataFrame(data)

# DataFrame 출력
df.show()

# 나이를 기준으로 DataFrame 정렬 및 출력
df.orderBy(df.age.desc()).show()

# 도시별로 그룹화하여 카운트
df.groupBy("city").count().show()