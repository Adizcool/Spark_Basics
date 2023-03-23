from pyspark.sql import SparkSession
import time
import os
import json

sc = SparkSession.builder.appName("CSVRead").getOrCreate()
with open("paths.json") as p:
    data = json.load(p)

path = data["CrimePath"]

df = sc.read.option("header", True).csv(path)

df.createOrReplaceTempView("crime")
sql = sc.sql("SELECT `LOCATION` FROM crime WHERE `Vict Age` = 30")
sql.show()
sql.explain()

start = time.time()

query = "`AREA` == 1 and `Vict Age` > 30"
sql = df.where(query)
sql.show()
print("Number of rows :", sql.count())

print("Time taken ", time.time() - start)

start = time.time()

partition_path = r'/Users/adityamanojbhaskaran/Data/Crime_Partition'
df.repartition(6).write.mode("overwrite").option("header", True) \
    .partitionBy("AREA").csv(partition_path)
partitioned_df = sc.read.option("header", True).csv(partition_path)

s = 0

path = r'/Users/adityamanojbhaskaran/Data/Crime_Partition/AREA=01'
for file in os.listdir(path):
    if file[0] == '.':
        continue
    print("File is %s" % file)
    part_df = sc.read.csv(path + "/" + file)
    print("Length of %s is %d" % (os.path.basename(path), part_df.count()))
    print("Current sum is %d" % (s + part_df.count()))
    s += part_df.count()

print("Time taken ", time.time() - start)
start = time.time()

query = partitioned_df.where("`AREA` == 1 and `Vict Age` > 30")
query.show()
print("Number of rows :", query.count())
query.explain()

print("Time taken ", time.time() - start)

partition_path = r'/Users/adityamanojbhaskaran/Data/Crime_Partition'
df.repartition("AREA").write.mode("overwrite").option("header", True) \
    .partitionBy("AREA").csv(partition_path)
partitioned_df = sc.read.option("header", True).csv(partition_path)

start = time.time()

query = partitioned_df.where("`AREA` == 1 and `Vict Age` > 30")
query.show()
print("Number of rows :", query.count())
query.explain()

print("Time taken ", time.time() - start)

partitioned_df.show()
print("Number of rows :", partitioned_df.count())
