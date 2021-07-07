import os
import argparse
from datetime import datetime
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t", "--treatments", help="treatments path", default="treatments"
    )
    parser.add_argument("-p", "--patients", help="patients path", default="patients")
    parser.add_argument(
        "-u",
        "--minio",
        help="minio url",
        default="https://minio.develop.vsmart00.com",
    )
    parser.add_argument("-l", "--loglevel", help="log level", default="ERROR")
    args = parser.parse_args()

    sq = (
        SparkSession.builder.appName("clever-join")
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
        .getOrCreate()
    )
    sq.sparkContext.setLogLevel(args.loglevel)

    treatments = sq.read.format("delta").load(args.treatments)
    patients = sq.read.format("delta").load(args.patients)

    treatments.show(truncate=False)
    patients.show(truncate=False)

    df0 = treatments.join(patients, "patient")
    df0.show(truncate=False)

    df0.filter(df0["sex"] == "1").groupBy("name").count().orderBy(
        "count", ascending=False
    ).show(truncate=False)
    df0.filter(df0["sex"] == "2").groupBy("name").count().orderBy(
        "count", ascending=False
    ).show(truncate=False)