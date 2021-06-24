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
    parser.add_argument("-l", "--loglevel", help="log level", default="ERROR")
    args = parser.parse_args()

    sq = SparkSession.builder.appName("query").getOrCreate()
    sq.sparkContext.setLogLevel(args.loglevel)

    ts0 = timeit.default_timer()

    df0 = (
        sq.read.format(args.inputformat)
        .option("recursiveFileLookup", "true")
        .load(args.input.split(","))
    )
    print("chart data")
    df0.show(truncate=False)
    '''
    df1 = (
        sq.read.format(args.inputformat)
        .option("recursiveFileLookup", "true")
        .load("parquet1".split(","))
    )
    print("receipt data")
    df1.show(truncate=False)
    print("join with patient id")
    df0.join(df1,df0["patient"]==df1["patient"]).where(df0["date"]==df1["date"]).select(df1["date"],df1["hospital"],df1["patient"],"name","price","exiting").show(truncate=False)
    print("the count of the treatment new patient taken")
    df0.join(df1,df0["patient"]==df1["patient"]).where(df0["date"]==df1["date"]).select(df1["date"],df1["hospital"],df1["patient"],"name","price","exiting").where(df1["exiting"]=="1").groupBy("name").count().orderBy("count",ascending=False).show(truncate=False)
    
    print("the price sum of the treatment new patient taken")
    df0.join(df1,df0["patient"]==df1["patient"]).where(df0["date"]==df1["date"]).select(df1["date"],df1["hospital"],df1["patient"],"name","price","exiting").where(df1["exiting"]=="1").groupBy("name").agg({"price": "sum"}).orderBy(
        "sum(price)", ascending=False
    ).show(truncate=False)
    print("the count of the treatment old patient taken")
    df0.join(df1,df0["patient"]==df1["patient"]).where(df0["date"]==df1["date"]).select(df1["date"],df1["hospital"],df1["patient"],"name","price","exiting").where(df1["exiting"]=="2").groupBy("name").count().orderBy("count",ascending=False).show(truncate=False)
    
    print("the price sum of the treatment old patient taken")
    df0.join(df1,df0["patient"]==df1["patient"]).where(df0["date"]==df1["date"]).select(df1["date"],df1["hospital"],df1["patient"],"name","price","exiting").where(df1["exiting"]=="2").groupBy("name").agg({"price": "sum"}).orderBy(
        "sum(price)", ascending=False
    ).show(truncate=False)
    '''
    df0.groupBy("name").count().orderBy("count", ascending=False).show(truncate=False)
    df0.groupBy("name").agg({"price": "sum"}).orderBy(
        "sum(price)", ascending=False
    ).show(truncate=False)

    ts1 = timeit.default_timer()

    print("runtime={}".format(ts1 - ts0))
