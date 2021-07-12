import os
import argparse
import timeit
from datetime import datetime,date
from pyspark import SparkContext, SQLContext
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, from_json, flatten, explode, expr,rank,months_between,datediff,to_date,current_date
from delta import *

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-if",
        "--inputformat",
        help="input format",
        default="delta",
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
    today = date.today()
    def cal_age(born):
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day)).cast("int")
    ts0 = timeit.default_timer()
    if os.path.exists("./patients"):
        pat_df0 = (
            sq.read.format(args.inputformat)
            .option("recursiveFileLookup", "true")
            .load("/".join(["patients"] + args.partitions.split(",")))
        )
        print("patient data")
        pat_df0=pat_df0.na.drop()
        pat_df0.show()
    else:
        print("there is no patient data")
        exit()

    if os.path.exists("./receipts"):
        rec_df0 = (
                sq.read.format(args.inputformat)
                .option("recursiveFileLookup","true")
                .load("/".join(["receipts"]+args.partitions.split(",")))
                )
        print("receipt data")
        rec_df0=rec_df0.na.drop()
        rec_df0.show()
    else:
        print("there is no receipt data")
        exit()

    if os.path.exists("./treatments"):
        trt_df0 = (
                sq.read.format(args.inputformat)
                .option("recursiveFileLookup","true")
                .load("/".join(["treatments"]+args.partitions.split(",")))
                )
        print("chart data")
        trt_df0=trt_df0.na.drop()
        trt_df0.show()
    else:
        print("there is no treatment data")
        exit()

    ts1 = timeit.default_timer()
    print("loadtime = {}".format(ts1-ts0))
    pat_df0.printSchema()
    print(pat_df0.count())
    pat_df0=pat_df0.withColumn("patientAge",(datediff(current_date(),col("birth"))/365.25).cast(IntegerType()))
    pat_df0=pat_df0.withColumn("patientGender",pat_df0["sex"])
    pat_df0.printSchema()
    pat_df0.orderBy("patientAge",asceding=False).show(truncate=False)
    df1=pat_df0.withColumn("newage",expr(
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
    df_join0=pat_df0.join(trt_df0,"patient")
    df_join0.printSchema()
    df_join0=df_join0.drop("birth","sex").distinct()
    
    df_join0.show()
    df2=df_join0.withColumn("newage",expr(
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
        )).cube("newage","patientGender","name").count().sort("newage","patientGender",col("count").desc())
    print("각 연령, 성별 받은 진료 수")
    df2.show()
    df2.explain(False)
    
    #이 부분은 parquet_faker1이라는 저장소가 존재해야됨.  parquet_faker1은 receipt data
    ''' 
    rec_df0=rec_df0.select("treatmentName","treatmentPrice")
    df3=df_join0.withColumn("newage",expr(
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
    df3=df3.filter(col("newage").isNotNull()).filter(col("patientGender").isNotNull()).filter(col("treatmentName").isNotNull()).select('*',rank().over(window).alias('rank')).filter(col('rank')<=3).sort("newage","patientGender",col("sum(treatmentPrice)").desc())
    df3.show(120,False)
    df3.explain(False)
    '''


    ts1 = timeit.default_timer()

    print("runtime={}".format(ts1 - ts0))
