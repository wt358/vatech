from datetime import datetime
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
                    "date",  TimestampType(), False
                ),
                StructField(
                    "content", StructType([StructField("tx", txSchema, True)]), True
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
                    "receiptDate", TimestampType(), False,
                ),
                StructField("newOrExistingPatient", StringType(), False),
            ]
        )
        return schema
    elif collection.endswith("patient"):
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("hospitalId", StringType(), True),
                StructField("sex", StringType(), True),
                StructField("birthDate", DateType(), True),
                StructField("newOrExistingPatient", StringType(), True),
            ]
        )
        return schema


def getCleverChartTreats(df0):
    timestamptodate = udf(lambda d: d.strftime("%Y%m%d"))

    df0 = df0.filter(df0["type"] == "TX")
    df0 = df0.withColumn("date1", timestamptodate(df0["date"]))
    df0 = df0.withColumn(
        "treats", explode(flatten(df0["content.tx.treatments.treats"]))
    )
    df0 = df0.withColumn("name", df0["treats.name"])
    df0 = df0.withColumn("price", df0["treats.price"])
    df0 = df0.select(
        df0["date1"].alias("date"),
        df0["hospitalId"].alias("hospital"),
        df0["patient"],
        df0["name"],
        df0["price"],
    ).orderBy("date", ascending=False)
    return df0


def getCleverReceipts(df0):
    timestamptodate = udf(lambda d: d.strftime("%Y%m%d"))

    df0 = df0.withColumn("date", timestamptodate(df0["receiptDate"]))
    df0 = df0.select(
        df0["date"],
        df0["hospitalId"].alias("hospital"),
        df0["patient"],
        df0["newOrExistingPatient"].alias("exiting"),
    ).orderBy("date", ascending=False)
    return df0


def getCleverPatients(df0):
    df0 = df0.select(
        df0["id"].alias("patient"),
        df0["hospitalId"].alias("hospital"),
        df0["sex"],
        df0["birthDate"].alias("birth"),
        df0["newOrExistingPatient"].alias("exiting"),
    )
    return df0
