import os
import argparse
from datetime import datetime
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, from_json, flatten, explode


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

    df0 = sq.read.format(args.inputformat).load(s3url)
    df0 = df0.filter(df0["operationType"] == "insert")
    df0 = df0.withColumn(
        "document", from_json(col("fullDocument"), getCleverSchema(args.collection))
    )
    df0 = df0.select("document.*")

    if args.target == "treats":
        df0 = getCleverChartTreats(df0)
    elif args.target == "receipt":
        df0 = getCleverReceipts(df0)
    df0.show(truncate=False)

    if args.partitions:
        df0.coalesce(1).write.partitionBy(args.partitions.split(",")).format(
            args.outputformat
        ).mode("overwrite").save(args.output)
    else:
        df0.coalesce(1).write.format(args.outputformat).mode("overwrite").save(
            args.output
        )