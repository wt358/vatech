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
    ts0 = timeit.default_timer()
    if os.path.exists("./patients"):
        pat_df0 = (
            sq.read.format(args.inputformat)
            .option("recursiveFileLookup", "true")
            .load("/".join(["patients"] + args.partitions.split(",")))
        )
        print("patient data")
        pat_df0=pat_df0.na.drop()
        print(pat_df0.count())
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
        print(rec_df0.count())
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
        print(trt_df0.count())
    else:
        print("there is no treatment data")
        exit()

    ts1 = timeit.default_timer()
    loadtime=ts1-ts0
    pat_df0=pat_df0.withColumn("patientGender",pat_df0["sex"]).drop("sex")
    pat_df0=pat_df0.withColumn("patientAge",(datediff(current_date(),col("birth"))/365.25).cast(IntegerType()))
    pat_df0.orderBy("patientAge",asceding=True).show(truncate=False)
    df0=pat_df0.withColumn("newage",expr(
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
        ))#df0는 환자정보에서 나이만 10로 끊어서
    df0.show()
    df1=df0.cube("newage","patientGender").count().sort("newage","patientGender")
    print("각 연령, 성별 등 방문 인원 수")
    df1.show()
    df_join0=df0.join(trt_df0,"patient")
    df_join0=df_join0.distinct()
    df_join0.show()
    df2=df_join0.cube("newage","patientGender","name").count().sort("newage","patientGender",col("count").desc())
    print("각 연령, 성별 받은 진료 수")
    df2.show()
    
    df3=df_join0.cube("newage","patientGender","name").agg({"price":"sum"}).sort("newage","patientGender",col("sum(price)").desc())
    
    df3.show()
    
    window=Window.partitionBy(df2['newage'],df2['patientGender']).orderBy(df2['newage'],df2['patientGender'],df2['count'].desc())
    print("각 연령, 성별 많이 받은 진료 탑 3")
    df4=df2.filter(col('newage').isNotNull()).filter(col('patientGender').isNotNull()).filter(df2['name'].isNotNull()).select('*',rank().over(window).alias('rank')).filter(col('rank')<=3).sort("newage","patientGender",col("count").desc())
    df4.show()
    
    window=Window.partitionBy(df3['newage'],df3['patientGender']).orderBy(df3['newage'],df3['patientGender'],df3['sum(price)'].desc())
    print("각 연령, 성별 가장 많은 매출을 낸 진료 탑 3")
    df5=df3.filter(col("newage").isNotNull()).filter(col("patientGender").isNotNull()).filter(col("name").isNotNull()).select('*',rank().over(window).alias('rank')).filter(col('rank')<=3).sort("newage","patientGender",col("sum(price)").desc())
    df5.show(120,False)
    
    df_join1=rec_df0.join(trt_df0,"patient").where(rec_df0["date"]==trt_df0["date"]).where(rec_df0["exiting"]=="1").distinct().groupBy("name").count().orderBy("count",ascending=False)
    df_join1.show(120,False)

    df_join2=rec_df0.join(trt_df0,"patient").where(rec_df0["date"]==trt_df0["date"]).where(rec_df0["exiting"]=="1").distinct().groupBy("name").agg({"price":"sum"}).orderBy("sum(price)",ascending=False)
    df_join2.show(120,False)
    ts1 = timeit.default_timer()
    print("df0 explain(환자 나이 계산)")
    df0.explain()
    print("df1 explain(각 연령, 성별별로 방문 인원)")
    df1.explain()
    print("df2 explain(각 연령, 성별별로 받은 진료 수)")
    df2.explain(True)
    print("df_join0 explain(join with treatment)")
    df_join0.explain()
    print("df3 explain(각 연령, 성별별 매출)")
    df3.explain()
    print("df4 explain(각 연령, 성별별로 치료 받는 진료 탑 3)")
    df4.explain()
    print("df5 explain(각 연령, 성별별로 매출 탑 3 진료)")
    df5.explain(False)
    print("df5 explain(처음 방문한 사람이 받은 진료 랭킹)")
    df_join1.explain(False)
    print("df5 explain(처음방문한 사람이 받은 진료 매출 랭킹)")
    df_join2.explain(False)

    print("loadtime = {}".format(loadtime))
    print("runtime={}".format(ts1 - ts0))
