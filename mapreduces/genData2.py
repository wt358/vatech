from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import udf, col, from_json, flatten, explode
from datetime import datetime
from faker import Faker
import random
import argparse

def getChart(df):
    df=df.withColumn("patientID",df["patient.patientID"])
    df=df.withColumn("patientAge",df["patient.age"])
    df=df.withColumn("patientGender",df["patient.sex"])
    df=df.withColumn("year",df["visit.date.year"])
    df=df.withColumn("month",df["visit.date.month"])
    df=df.withColumn("day",df["visit.date.day"])
    df=df.withColumn("hospitalName",df["hospital.hospitalID"])
    df=df.withColumn("treatmentName",df["treatment.name"])
    df = df.select(
        df["patientID"],
        df["patientAge"],
        df["patientGender"],
        df["hospitalName"],
        df["doctor"],
        df["treatmentName"],
        df["year"],
        df["month"],
        df["day"],
    ).orderBy("patient", ascending=False)
    return df


def getReceipt(df):
    df=df.withColumn("hospitalName",df["hospital.hospitalID"])
    df=df.withColumn("hospitalAddress",df["hospital.address"])
    df=df.withColumn("hospitalContact",df["hospital.contact"])
    df=df.withColumn("year",df["visit.date.year"])
    df=df.withColumn("month",df["visit.date.month"])
    df=df.withColumn("day",df["visit.date.day"])
    df=df.withColumn("patientID",df["patient.patientID"])
    df=df.withColumn("treatmentName",df["treatment.name"])
    df=df.withColumn("treatmentPrice",df["treatment.price"])
    df = df.select(
        df["hospitalName"],
        df["hospitalAddress"],
        df["hospitalContact"],
        df["year"],
        df["month"],
        df["day"],
        df["patientID"],
        df["treatmentName"],
        df["treatmentPrice"],
        df["payment"],
    ).orderBy("patient", ascending=False)
    return df

if __name__=="__main__":
    #데이터 생성
    sc = SparkContext("local","fake data")

    spark=SparkSession.builder.appName("generate fake data").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    schema = StructType([
             StructField("patient", StructType([
                 StructField("patientID", StringType(), True), StructField("age", IntegerType(), True), StructField("sex", StringType(), True)]), True),
             StructField("doctor", StringType(), True),
             StructField("hospital", StructType([
                 StructField("hospitalID", StringType(), True), StructField("address", StringType(), True), StructField("contact", StringType(), True)]), True),
             StructField("treatment", StructType([
                 StructField("name", StringType(), True), StructField("price", IntegerType(), True)]), True),
             StructField("visit", StructType([
                 StructField("date", StructType([
                     StructField("year", IntegerType(), True), StructField("month", IntegerType(), True), StructField("day", IntegerType(), True)]), True)]), True),
             StructField("payment", StringType(), True)])

    fake=Faker()

    list_sex=["male", "female"]
    list_hospital=[("Vatech","75-11 Seokwoo-dong, Hwaseong-si, Gyeonggi-do","031-379-9500"),
    ("Samsung","81 Ilwon-ro, Ilwon-dong, Gangnam-gu, Seoul","1599-3114"),
    ("Asan","88 Olympic-ro 43-gil, Pungnap 2-dong, Songpa-gu, Seoul","1688-7575")]
    list_patient=[]
    list_doctor=[]
    for i in range(20000):
        list_patient.append((fake.uuid4(),fake.pyint(10,80),fake.word(list_sex)))
    
    for i in range(500):
        list_doctor.append(fake.uuid4())
    
    list_treatment=[("Abutment Tightening & Impression",0),("Amalgam Filling",1600000),
    ("Anterior Teeth Resin",7000000),("Basic Care",0),
    ("Bite Wing X-Ray",100000),("CBCT",250000),("Canal Enlargement/Shaping",2000000),
    ("Canal Filling",6000000),("Canal Irrigation",1000000),
    ("Cervical Resin",400000),("Deciduous Tooth Extraction",400000),
    ("Dental Pulp Expiration",500000),("Dressing",0),("GI Filling",300000),
    ("Kontact (France) Implant",175000000),("Metal Braces",45000000),
    ("Neo Biotech SL Implant",60000000),("Oral Exam",0),
    ("Oral Prophylaxis",0),("Orthodontics Diagnosis",0),("PFM-Ni",6000000),("Panorama",400000),
    ("Periapical X-Ray",100000),("Periodonatal Curettage",200000),
    ("Periodontal Probing Depths",0),("Permanent Tooth Extraction",1000000),
    ("Posterial Teeth Resin",500000),
    ("Prosthesis Re-attachment",200000),("Pulpectomy",6000000),("Re-Endo",12000000),
    ("Root Planing",500000),("Scailing",500000),("Simple Bone Graft",4000000),
    ("Temporary Crown",0),("Wisdom Tooth Extraction",6000000),("Working Length",0)          ]

    list_payment=["cash","card"]

    tmp=[]
    for i in range(15000):
        hospital=random.choice(list_hospital)
        if hospital[1]=="Vatech":
            doctor=random.choice(list_doctor[:200])
        elif hospital[1]=="Samsung":
            doctor=random.choice(list_doctor[200:350])
        else :
            doctor=random.choice(list_doctor[350:])
        patient=random.choice(list_patient)
        treatment=random.choice(list_treatment)
        date=fake.date_between(start_date="-2y")
        year=date.year
        month=date.month
        day=date.day
        if treatment[1]==0:
            payment="-"
        else:
            payment=random.choice(list_payment)
        tmp.append((patient, doctor, hospital, treatment, [(year, month, day)], payment))
    fakeData=spark.createDataFrame(data=tmp,schema=schema)
    fakeData.printSchema()
    print("original")
    fakeData.show(50)

#인자
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-y", "--year", help="target year", type=int, default=datetime.today().year)
    parser.add_argument("-m", "--month", help="target month", type=int, default=datetime.today().month)
    parser.add_argument("-d", "--day", help="target day", type=int, default=datetime.today().day)
    parser.add_argument("-t", "--target", help="target info", default="chart")
    parser.add_argument("-of", "--outputformat", help="output format", default="parquet")
    parser.add_argument("-o", "--output", help="output path", default="parquet")
    
    args = parser.parse_args()

    if args.target == "chart":
        fakeData = getChart(fakeData)
    elif args.target == "receipt":
        fakeData = getReceipt(fakeData)
    print("edited")
    fakeData.printSchema()
    fakeData.show(50)

    fakeData.coalesce(1).write.format(args.outputformat).mode("overwrite").save(args.output)

