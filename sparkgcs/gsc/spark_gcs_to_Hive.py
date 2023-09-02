from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import date
from google.cloud import storage

import os

def main():
# create spark session object
    spark = SparkSession.builder\
                        .appName("HDFS - GCP GCS Read/Write") \
                        .config("spark.jars","/home/hduser/install/gcs-connector-hadoop2-2.2.7.jar")\
                        .enableHiveSupport()\
                        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sc=spark.sparkContext
# spark configuration
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    conf.set("google.cloud.auth.service.account.json.keyfile","/home/hduser/install/projectbigdata-395203-26f51bbb59ce.json")
    print("Data Transfer between HDFS to GCS and Vice versa")
# hive tble name
#    hdfs_path="hdfs://localhost:54310/user/hduser/datatotransfer/"
    hive_table="gcs.customer"

#getting current date
    curr_date =date.today().strftime("%m%d%y")
    file_name = "custs_"+curr_date
    gcs_path = "gs://bigdatavicky/"+file_name

#service account to access gc with the private key
    path_to_private_key="/home/hduser/install/projectbigdata-395203-26f51bbb59ce.json"
#check the file existence in the google cloud storage using google cloud storage api
    client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
    bucket = storage.Bucket(client,'bigdatavicky')
    blob = bucket.blob(file_name)
    if blob.exists():
        custstructtype1 = StructType([StructField("id", IntegerType(), False),
                                      StructField("custfname", StringType(), False),
                                      StructField("custlname", StringType(), True),
                                      StructField("custage", ShortType(), True),
                                      StructField("custprofession", StringType(), True)])
        gcs_df = spark.read.csv(gcs_path,schema=custstructtype1,mode="dropmalformed")
        gcs_df.printSchema()
        gcs_df.show(3)
        print("GCS read is successful")
        print("saving to Hive Table is starting....")
        gcs_df.write.mode("overwrite").saveAsTable(hive_table)
        print("saving to Hive Table is completed....")
    else:
        print("File is not available")

main()