'''
  1. java -jar metabase.jar
  2. $SPARK_HOME \ ./sbin/start-thriftserver.sh
  3. beeline \ !connect jdbc:hive2://localhost:10000
  4. spark-submit jdbc.py
'''
from pyspark.sql import *

spark = SparkSession.builder.appName("Dataframe to metabase using Thrift JDBC Server")\
  .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("parquet").load("./parsed/parquet")
print("Parsed Data")
df.show()

# sqlCtx=HiveContext(spark.sparkContext)
# df.getOrCreateTempView("Dummy")

db_url = "jdbc:hive2://localhost:10000/default"
my_properties = {
  "user" : "songhyun"
}

df.write.format("jdbc").option("url", db_url).option("dbtable", "dummy").save()