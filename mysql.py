# spark-submit --driver-class-path=path/to/mysql-connector-java-8.0.25.jar mysql.py
# $SPARK_HOME/jars에 jdbc connector 파일 넣어두기

from pyspark.sql import SparkSession
from pyspark import SparkContext
import timeit
spark = SparkSession.builder\
  .appName("Mysql JDBC")\
  .config("spark.driver.extraClassPath",
    "$SPARK_HOME/jars/mysql-connector-java-8.0.25.jar")\
  .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sql_url = "localhost"
user = "spark"
password = "spark"
database = "sql_study"
table = "test"

jdbc = spark.read.format("parquet").load("./parsed/parquet")
jdbc.show()

start = timeit.default_timer()

jdbc.write.format("jdbc")\
  .option("driver", "com.mysql.cj.jdbc.Driver")\
  .option("url", "jdbc:mysql://mysql.it.vsmart00.com:3306/faker?serverTimezone=Asia/Seoul&useServerPrepStmts=false&rewriteBatchedStatements=true")\
  .option("user", "root")\
  .option("password", "haru1004")\
  .option("dbtable", "dummy_clever")\
  .mode("overwrite")\
  .save()

end = timeit.default_timer()

print(str(end - start) + " time.")

'''
jdbc.write.format("jdbc")\
  .option("driver", "com.mysql.cj.jdbc.Driver")\
  .option("url", "jdbc:mysql://{}:3306/{}?serverTimezone=Asia/Seoul&useServerPrepStmts=false&rewriteBatchedStatements=true".format(sql_url, database))\
  .option("user", user)\
  .option("password", password)\
  .option("dbtable", table)\
  .mode("overwrite")\
  .save()
'''