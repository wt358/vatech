import os
import argparse
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json


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
    parser.add_argument("-y", "--year", help="target year", type=int, default=2021)
    parser.add_argument("-m", "--month", help="target month", type=int, default=6)
    parser.add_argument("-d", "--day", help="target day", type=int, default=18)
    parser.add_argument(
        "-of", "--outputformat", help="output format", default="parquet"
    )
    parser.add_argument("-o", "--output", help="output path", default="parquet")
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
        SparkSession.builder.appName("PySpark")
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.environ.get("MINIO_ACCESS_KEY", "haruband"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.environ.get("MINIO_SECRET_KEY", "haru1004"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", args.minio)
        .config("es.nodes", args.elasticsearch)
        .config("es.nodes.discovery", "true")
        .getOrCreate()
    )
    sq.sparkContext.setLogLevel(args.loglevel)

    df0 = sq.read.format(args.inputformat).load(
        "s3a://{}/topics/{}/year={:04d}/month={:02d}/day={:02d}".format(
            args.bucket, args.collection, args.year, args.month, args.day
        )
    )
    df0.printSchema()
    df0.show()

    df0 = df0.filter(df0["operationType"] == "insert")
    df0.show()

    trSchema = StructType([StructField("name", StringType(), False)])
    tmSchema = StructType(
        [
            StructField("name", StringType(), False),
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
    chartSchema = StructType(
        [
            StructField("type", StringType(), False),
            StructField(
                "content", StructType([StructField("tx", txSchema, False)]), False
            ),
        ]
    )

    df0 = df0.withColumn("document", from_json(col("fullDocument"), chartSchema))
    df0 = df0.filter(df0["document.type"] == "TX")
    df0 = df0.withColumn("treatments", df0["document.content.tx.treatments.treats"])
    df0.printSchema()
    df0.select(df0["treatments"]).show(truncate=False)

    df0.coalesce(1).write.format(args.outputformat).mode("overwrite").save(args.output)