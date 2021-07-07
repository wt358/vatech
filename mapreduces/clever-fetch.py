import os
import argparse
from datetime import datetime
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf, col, from_json, flatten, explode
from faker import Faker
from faker.providers import internet
from datetime import date
from pandas import DataFrame
from delta import *
patient_list=[
            "5819294f-bd03-4f54-bf6a-1ceea59f331b",
            "79d30a59-3019-4aa9-b1f4-de716250f81e",
            "dafb8840-14cb-4b4e-9020-487011abacae",
            "4d060a7d-ad40-4305-905e-afa1feeb14f9",
            "fbbd93ef-72bd-4621-9597-8b3324ed1aca",
            "eac0e1b9-4c68-4138-a89c-5a5f1ce7c6a3",
            "c953c6a1-5fac-4c4a-89a3-b472c4e25181",
            "4d060a7d-ad40-4305-905e-afa1feeb14f9",
        ]
fake=Faker()
for i in range(250):
    patient_list.append(fake.uuid4())


def getCleverSchema(collection):
    if collection.endswith("chart"):
        trSchema = StructType(
            [
                StructField("name", StringType(), False),
                StructField("price", IntegerType(), False),
            ]
        )
        tmSchema = StructType(
            [
                StructField(
                    "treats",
                    ArrayType(
                        trSchema,
                        False,
                    ),
                    False,
                ),
            ]
        )
        txSchema = StructType(
            [
                StructField(
                    "treatments",
                    ArrayType(
                        tmSchema,
                        False,
                    ),
                    False,
                )
            ]
        )
        schema = StructType(
            [
                StructField("hospitalId", StringType(), False),
                StructField("patient", StringType(), False),
                StructField("type", StringType(), False),
                StructField(
                    "date", StructType([StructField("$date", IntegerType(), False)])
                ),
                StructField(
                    "content", StructType([StructField("tx", txSchema, False)]), False
                ),
            ]
        )
        return schema
    elif collection.endswith("receipt"):
        schema = StructType(
            [
                StructField("hospitalId", StringType(), False),
                StructField("patient", StringType(), False),
                StructField(
                    "receiptDate",
                    StructType([StructField("$date", IntegerType(), False)]),
                ),
                StructField("newOrExistingPatient", StringType(), False),
            ]
        )
        return schema


def getCleverChartTreats(df0):
    timestamptodate = udf(lambda d: datetime.fromtimestamp(d).strftime("%Y%m%d"))

    df0 = df0.filter(df0["type"] == "TX")
    df0 = df0.withColumn("date", timestamptodate(df0["date.$date"]))
    df0 = df0.withColumn(
        "treats", explode(flatten(df0["content.tx.treatments.treats"]))
    )
    df0 = df0.withColumn("name", df0["treats.name"])
    df0 = df0.withColumn("price", df0["treats.price"])
    df0 = df0.select(
        df0["date"],
        df0["hospitalId"].alias("hospital"),
        df0["patient"],
        df0["name"],
        df0["price"],
    ).orderBy("date", ascending=False)
    return df0


def getCleverReceipts(df0):
    timestamptodate = udf(lambda d: datetime.fromtimestamp(d).strftime("%Y%m%d"))

    df0 = df0.withColumn("date", timestamptodate(df0["receiptDate.$date"]))
    df0 = df0.select(
        df0["date"],
        df0["hospitalId"].alias("hospital"),
        df0["patient"],
        df0["newOrExistingPatient"].alias("exiting"),
    ).orderBy("date", ascending=False)
    return df0

def genFakeChartData(df0,name_list,price_dic):
    today=date.today().strftime("%Y%m%d")
    hospital="vatech"
    spark = SparkSession.builder.appName("newdata").getOrCreate()
    newRow=[]
    for i in range(150000):
        patientID=fake.word(ext_word_list=patient_list)
        name=fake.word(ext_word_list=name_list)
        price = int(price_dic[name])
        newRow1=[today,hospital,patientID,name,price]
        newRow.append(newRow1)
    df1 = spark.createDataFrame(newRow,['date','hospital','patient','name','price'])
    df0 = df1.union(df0)
    return df0
def genFakeReceiptData(df0,exiting_dic):
    today=date.today().strftime("%Y%m%d")
    hospital="vatech"
    spark = SparkSession.builder.appName("newdata").getOrCreate()
    newRow=[]
    for i in range(150000):
        patientID=fake.word(ext_word_list=patient_list)
        if patientID in exiting_dic:
            exiting = exiting_dic[patientID]
        else:
            exiting_dic[patientID]=2;
            exiting = 1;
        newRow1=[today,hospital,patientID,exiting]
        newRow.append(newRow1)
    df1 = spark.createDataFrame(newRow,['date','hospital','patient','exiting'])
    df0 = df1.union(df0)
    return df0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-if",
        "--inputformat",
        help="input format",
        default="json",
    )
    parser.add_argument("-b", "--bucket", help="target bucket", default="mongodb")
    parser.add_argument(
        "-c", "--collection", help="target collection", default="clever.dev0-chart"
    )
    parser.add_argument(
        "-y", "--year", help="target year", type=int, default=datetime.today().year
    )
    parser.add_argument(
        "-m", "--month", help="target month", type=int, default=datetime.today().month
    )
    parser.add_argument(
        "-d", "--day", help="target day", type=int, default=datetime.today().day
    )
    parser.add_argument("-t", "--target", help="target info", default="treats")
    parser.add_argument(
        "-of", "--outputformat", help="output format", default="parquet"
    )
    parser.add_argument("-o", "--output", help="output path", default="parquet")
    parser.add_argument("-p", "--partitions", help="output partitions")
    parser.add_argument(
        "-u",
        "--minio",
        help="minio url",
        default="https://minio.develop.vsmart00.com",
    )
    parser.add_argument(
        "-e",
        "--elasticsearch",
        help="elasticsearch url",
        default="localhost:9200",
    )
    parser.add_argument("-l", "--loglevel", help="log level", default="ERROR")
    args = parser.parse_args()

    sq = (
        SparkSession.builder.appName("clever-fetch")
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.environ.get("MINIO_ACCESS_KEY", "haruband"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.environ.get("MINIO_SECRET_KEY", "haru1004"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", args.minio)
        .config("es.nodes", args.elasticsearch)
        .config("es.nodes.discovery", "true")
        .config("es.index.auto.create", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    sq.sparkContext.setLogLevel(args.loglevel)

    s3url = "s3a://{}/topics/{}".format(args.bucket, args.collection)
    if args.year > 0:
        s3url = s3url + "/year={:04d}".format(args.year)
        if args.month > 0:
            s3url = s3url + "/month={:02d}".format(args.month)
            if args.day > 0:
                s3url = s3url + "/day={:02d}".format(args.day)
    print("today is ")
    print(date.today().strftime("%Y%m%d"))
    

    df0 = sq.read.format(args.inputformat).load(s3url)
    df0.printSchema()
    df0 = df0.filter(df0["operationType"] == "insert")
    df0 = df0.withColumn(
        "document", from_json(col("fullDocument"), getCleverSchema(args.collection))
    )
    df0 = df0.select("document.*")
    df0.printSchema()

    if args.target == "treats":
        df0 = getCleverChartTreats(df0)
        name_list = [(row.name) for row in df0.select("name").collect()]
        price_dic = {row.name:row.price for row in df0.select("name","price").collect()}
        df0=genFakeChartData(df0,name_list,price_dic)
    elif args.target == "receipt":
        df0 = getCleverReceipts(df0)
        exiting_dic = {row.patient:2 for row in df0.select("patient").collect()}
        df0 = genFakeReceiptData(df0,exiting_dic)
    df0.printSchema()
    df0.show(truncate=False)
    name_list = [(row.name) for row in df0.select("name").collect()]

'''
    price_dic = {row.name:row.price for row in df0.select("name","price").collect()}
    df0=genFakeChartData(df0,name_list,price_dic)
    df0.orderBy("date",ascending=False).show(truncate=False)
    print(df0.count())
'''
    print(df0.count())
    if args.partitions:
        df0.coalesce(1).write.partitionBy(args.partitions.split(",")).format(
            args.outputformat
        ).mode("overwrite").save(args.output)
    else:
        df0.coalesce(1).write.format(args.outputformat).mode("overwrite").save(
            args.output
        )
