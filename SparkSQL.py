import pyspark.sql as sql
from pyspark.sql.functions import col, translate
import json

sc = sql.SparkSession.builder.appName("SparkSQL").getOrCreate()
with open("paths.json") as p:
    data = json.load(p)
path = data["WorldPath"]
df = sc.read.option("header", True).csv(path)
df.printSchema()

df = df.withColumn("Yearly Change", translate("Yearly Change", '%', '').cast("Float")) \
    .withColumn("urban", translate("Urban", '%', '').cast("Float")) \
    .withColumn("World Share", translate("World Share", '%', '').cast("Float")) \
    .withColumn("Population(2020)", translate("Population(2020)", ',', '').cast("Integer")) \
    .withColumn("Land Area(km^2)", translate("Land Area(km^2)", ',', '').cast("Integer")) \
    .withColumn("Migrants", translate("Migrants", ',', '').cast("Integer"))
df.show(10)

df.select("Country or Dependency", "`Population(2020)`", "Yearly Change").where("`Yearly Change` > 1.5") \
    .orderBy(col("`Population(2020)`").desc()).show(10)
temp = df.groupby("Regions", "Population(2020)").count().where("`Population(2020)` > 150000000")
temp.groupby("Regions").count().orderBy(col("count").desc()).show()
