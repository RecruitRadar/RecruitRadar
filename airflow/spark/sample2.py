from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# SparkSession 초기화
spark = SparkSession.builder.appName("ExampleApp2").getOrCreate()

# 데이터 정의
arrayStructureData = [
 (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
 (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
 (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
 (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
 (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
 (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
]

# 스키마 정의
arrayStructureSchema = StructType([
 StructField('name', StructType([
 StructField('firstname', StringType(), True),
 StructField('middlename', StringType(), True),
 StructField('lastname', StringType(), True)
 ])),
 StructField('languages', ArrayType(StringType()), True),
 StructField('state', StringType(), True),
 StructField('gender', StringType(), True)
])

# DataFrame 생성
df = spark.createDataFrame(data = arrayStructureData, schema = arrayStructureSchema)

# 스키마와 데이터 출력
df.printSchema()
df.show()