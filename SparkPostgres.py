from pyspark.sql import SparkSession, Row
import json

with open("paths.json") as p:
    data = json.load(p)

sc = SparkSession.builder.appName("SparkPostgres").master("local") \
    .config("spark.driver.extraClassPath", data["PostgresClassPath"]).getOrCreate()

# config("spark.jars", data["PostgresJarPath"]) - For JAR files
# extraClassPath for ClassPath

df = sc.read.format("jdbc"). \
    options(
    url='jdbc:postgresql://localhost:5432/testing',
    dbtable='t1',
    user='postgres',
    password='Aruba@123',
    driver='org.postgresql.Driver'). \
    load()

df.printSchema()
df.select('*').collect()

student_df = sc.createDataFrame([
    Row(id=1, name='Aditya', marks=97),
    Row(id=2, name='Anand', marks=79),
    Row(id=3, name='John', marks=86),
    Row(id=4, name='Bob', marks=94),
    Row(id=5, name='Shirley', marks=34),
])
student_df.show()

student_df.write.format("jdbc").options(url='jdbc:postgresql://localhost:5432/testing',
                                        driver="org.postgresql.Driver", dbtable="students",
                                        user=data["PostgresUser"],
                                        password=data["PostgresPass"],
                                        header=True).mode("overwrite").save()
