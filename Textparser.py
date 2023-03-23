from pyspark.sql import SparkSession
import json

sc = SparkSession.builder.appName("TextParser").getOrCreate()
with open("paths.json") as p:
    data = json.load(p)
filePath = data["TextPath"]

df = sc.read.text(filePath)
parseWord = input("Provide the word\t")

number = df.filter(df.value.contains(parseWord)).count()
print("Lines with %s : %d" % (parseWord, number))
