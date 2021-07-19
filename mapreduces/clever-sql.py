import os
import argparse
import timeit
from datetime import datetime
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, from_json, flatten, explode


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-if",
        "--inputformat",
        help="input format",
        default="parquet",
    )
    parser.add_argument("-i", "--input", help="input path", default="parquet")
    parser.add_argument("-p", "--partitions", help="input partitions", default="")
    parser.add_argument("-f", "--filters", help="input filters")
    parser.add_argument("-l", "--loglevel", help="log level", default="ERROR")
    args = parser.parse_args()

    sq = SparkSession.builder.appName("clever-sql").getOrCreate()
    sq.sparkContext.setLogLevel(args.loglevel)

    ts0 = timeit.default_timer()

    df0 = (
        sq.read.format(args.inputformat)
        .option("recursiveFileLookup", "true")
        .load("/".join([args.input] + args.partitions.split(",")))
    )
    if args.filters:
        for kv in args.filters.split(","):
            k, v = kv.split("=")
            df0 = df0.filter(df0[k] == v)
    df0.createOrReplaceTempView("clever")

    sq.sql("select name, count(name) as count from clever group by name").show(
        truncate=False
    )
    sq.sql("select name, sum(price) as sum from clever group by name").show(
        truncate=False
    )

    ts1 = timeit.default_timer()

    print("runtime={}".format(ts1 - ts0))
