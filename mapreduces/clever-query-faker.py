import os
import argparse
import timeit
from datetime import datetime
from pyspark import SparkContext, SQLContext
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, from_json, flatten, explode, expr,rank


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
    parser.add_argument(
        "-u",
        "--minio",
        help="minio url",
        default="https://minio.develop.vsmart00.com",
    )
    parser.add_argument("-l", "--loglevel", help="log level", default="ERROR")
    args = parser.parse_args()

    sq = (
        SparkSession.builder.appName("clever-query")
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

    ts0 = timeit.default_timer()

    df0 = (
        sq.read.format(args.inputformat)
        .option("recursiveFileLookup", "true")
        .load("/".join([args.input] + args.partitions.split(",")))
    )
    if os.path.exists("./parquet_faker1"):
        rec_df0 = (
                sq.read.format(args.inputformat)
                .option("recursiveFileLookup","true")
                .load("/".join(["parquet_faker1"]+args.partitions.split(",")))
                )
        rec_df0.show()
    ts1 = timeit.default_timer()
    print("loadtime = {}".format(ts1-ts0))
    print("chart data")
    df0.show(truncate=False)
    df0.explain(False)
    df1=df0.withColumn("newage",expr(
        "case when patientAge<10 then '0~10'"
        +"when patientAge<20 then '10~20'"
        +"when patientAge<30 then '20~30'"
        +"when patientAge<40 then '30~40'"
        +"when patientAge<50 then '40~50'"
        +"when patientAge<60 then '50~60'"
        +"when patientAge<70 then '60~70'"
        +"when patientAge<80 then '70~80'"
        +"when patientAge<90 then '80~90'"
        +"when patientAge<100 then '90~100'"
        +"else 'Very Old' end"
        )).cube("newage","patientGender").count().sort("newage","patientGender")
    print("각 연령, 성별 등 방문 인원 수")
    df1.show()
    df1.explain()
    df2=df0.withColumn("newage",expr(
        "case when patientAge<10 then '0~10'"
        +"when patientAge<20 then '10~20'"
        +"when patientAge<30 then '20~30'"
        +"when patientAge<40 then '30~40'"
        +"when patientAge<50 then '40~50'"
        +"when patientAge<60 then '50~60'"
        +"when patientAge<70 then '60~70'"
        +"when patientAge<80 then '70~80'"
        +"when patientAge<90 then '80~90'"
        +"when patientAge<100 then '90~100'"
        +"else 'Very Old' end"
        )).cube("newage","patientGender","treatmentName").count().sort("newage","patientGender",col("count").desc())
    print("각 연령, 성별 받은 진료 수")
    df2.show()
    df2.explain(False)
    
    #이 부분은 parquet_faker1이라는 저장소가 존재해야됨.  parquet_faker1은 receipt data
    '''
    rec_df0=rec_df0.select("treatmentName","treatmentPrice")
    df3=df0.withColumn("newage",expr(
        "case when patientAge<10 then '0~10'"
        +"when patientAge<20 then '10~20'"
        +"when patientAge<30 then '20~30'"
        +"when patientAge<40 then '30~40'"
        +"when patientAge<50 then '40~50'"
        +"when patientAge<60 then '50~60'"
        +"when patientAge<70 then '60~70'"
        +"when patientAge<80 then '70~80'"
        +"when patientAge<90 then '80~90'"
        +"when patientAge<100 then '90~100'"
        +"else 'Very Old' end"
        )).join(rec_df0,rec_df0['treatmentName']==df0['treatmentName']).drop(rec_df0['treatmentName']).distinct().cube("newage","patientGender","treatmentName").agg({"treatmentPrice":"sum"}).sort("newage","patientGender",col("sum(treatmentPrice)").desc())
    
    #df3.show()
    window=Window.partitionBy(df2['newage'],df2['patientGender']).orderBy(df2['newage'],df2['patientGender'],df2['count'].desc())
    print("각 연령, 성별 많이 받은 진료 탑 3")
    df2.join(rec_df0,rec_df0['treatmentName']==df2['treatmentName']).distinct().filter(col('newage').isNotNull()).filter(col('patientGender').isNotNull()).filter(df2['treatmentName'].isNotNull()).select('*',rank().over(window).alias('rank')).filter(col('rank')<=3).sort("newage","patientGender",col("count").desc()).show(120,truncate=False)
    window=Window.partitionBy(df3['newage'],df3['patientGender']).orderBy(df3['newage'],df3['patientGender'],df3['sum(treatmentPrice)'].desc())
    print("각 연령, 성별 가장 많은 매출을 낸 진료 탑 3")
    df3.filter(col("newage").isNotNull()).filter(col("patientGender").isNotNull()).filter(col("treatmentName").isNotNull()).select('*',rank().over(window).alias('rank')).filter(col('rank')<=3).sort("newage","patientGender",col("sum(treatmentPrice)").desc()).show(120,truncate=False)
    '''
    


    ts1 = timeit.default_timer()

    print("runtime={}".format(ts1 - ts0))
