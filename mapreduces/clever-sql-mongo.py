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
        pat_df0.createOrReplaceTempView("patient")
        sq.sql("select* from patient where birth is not null and sex is not null and hospital is not null and patient is not null and exiting is not null").createOrReplaceTempView("patient")
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
        rec_df0.createOrReplaceTempView("receipt")
        sq.sql("select* from receipt where date is not null and hospital is not null and patient is not null and exiting is not null").createOrReplaceTempView("receipt")
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
        trt_df0.createOrReplaceTempView("treatment")
        sq.sql("select* from treatment where date is not null and hospital is not null and patient is not null and name is not null and price is not null").createOrReplaceTempView("treatment")
    else:
        print("there is no treatment data")
        exit()


    ts1 = timeit.default_timer()
    loadtime=ts1-ts0
    sq.sql("select patient,hospital, sex as patientGender,birth,exiting,floor(datediff(current_date(),birth)/365.25) as patientAge  from patient").createOrReplaceTempView("df0")
    sq.sql("select *, case when patientAge<10 then '0~10'when patientAge<20 then '10~20'when patientAge<30 then '20~30'when patientAge<40 then '30~40'when patientAge<50 then '40~50'when patientAge<60 then '50~60'when patientAge<70 then '60~70'when patientAge<80 then '70~80' when patientAge<90 then '80~90' when patientAge<100 then '90~100' else 'Very Old' end as newage from df0").createOrReplaceTempView("df0")
    sq.sql("select * from df0").show()
    sq.sql("select newage, patientGender, count(patient) from df0 group by cube (newage,patientGender) order by(newage, patientGender)").createOrReplaceTempView("df1")
    sq.sql("select * from df1").show()
    print("각 연령, 성별 등 방문 인원 수")
    sq.sql("select distinct df0.patient, df0.hospital, patientGender, birth, exiting, patientAge, newage, date, name, price from df0 inner join treatment on df0.patient = treatment.patient").createOrReplaceTempView("df_join0")
    sq.sql("select * from df_join0").show()
    sq.sql("select newage, patientGender, name, count(patient) as count from df_join0 group by cube (newage,patientGender,name) order by newage asc,patientGender asc,count desc").createOrReplaceTempView("df2")
    sq.sql("select * from df2").show()
    sq.sql("select newage, patientGender, name, sum(price) as sum from df_join0 group by cube (newage,patientGender,name) order by newage asc,patientGender asc,sum desc").createOrReplaceTempView("df3")
    sq.sql("select * from df3").show()
    
    print("각 연령, 성별 많이 받은 진료 탑 3")
    sq.sql("select * from (select newage, patientGender, name, count, rank() over(partition by newage, patientGender order by count desc ) as rank from df2 where newage is not null and patientGender is not null order by newage asc, patientGender asc,rank asc) where rank <=3 ").createOrReplaceTempView("df4")
    sq.sql("select * from df4").show(200)
    
    print("각 연령, 성별 가장 많은 매출을 낸 진료 탑 3")
    sq.sql("select * from (select newage, patientGender, name, sum, rank() over(partition by newage, patientGender order by sum desc ) as rank from df3 where newage is not null and patientGender is not null order by newage asc, patientGender asc,rank asc) where rank <=3 ").createOrReplaceTempView("df5")
    sq.sql("select * from df5").show(200)

    sq.sql("select name, count(name) as count from receipt inner join treatment on receipt.patient==treatment.patient where receipt.date = treatment.date and receipt.exiting == 1 group by name order by count desc").createOrReplaceTempView("df_join1")
    sq.sql("select * from df_join1").show()

    sq.sql("select name, sum(price) as sum from receipt inner join treatment on receipt.patient==treatment.patient where receipt.date = treatment.date and receipt.exiting == 1 group by name order by sum desc").createOrReplaceTempView("df_join2")
    sq.sql("select * from df_join2").show()
    ts1 = timeit.default_timer()
    
    print("df0 explain(환자 나이 계산)")
    sq.sql("explain select*from df0").show(truncate=False)
    print("df1 explain(각 연령, 성별별로 방문 인원)")
    sq.sql("explain select*from df1").show(truncate=False)
    print("df2 explain(각 연령, 성별별로 받은 진료 수)")
    sq.sql("explain select*from df2").show(truncate=False)
    print("df_join0 explain(join with treatment)")
    sq.sql("explain select*from df_join0").show(truncate=False)
    print("df3 explain(각 연령, 성별별 매출)")
    sq.sql("explain select*from df3").show(truncate=False)
    print("df4 explain(각 연령, 성별별로 치료 받는 진료 탑 3)")
    sq.sql("explain select*from df4").show(truncate=False)
    print("df5 explain(각 연령, 성별별로 매출 탑 3 진료)")
    sq.sql("explain select*from df5").show(truncate=False)
    print("df5 explain(처음 방문한 사람이 받은 진료 랭킹)")
    sq.sql("explain select*from df_join1").show(truncate=False)
    print("df5 explain(처음방문한 사람이 받은 진료 매출 랭킹)")
    sq.sql("explain select*from df_join2").show(truncate=False)
    
    print("loadtime = {}".format(loadtime))
    print("runtime={}".format(ts1 - ts0))
