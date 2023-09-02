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
# file path and file names
    hdfs_path=""

#getting current date
    curr_date =date.today().strftime("%m%d%y")
    file_name =  "custs_"+curr_date
    gcs_path = "gs://bigdatavicky/"+file_name

#service account to access gc with the private key
    path_to_private_key="/home/hduser/install/projectbigdata-395203-26f51bbb59ce.json"
    client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
    bucket = storage.Bucket(client,'bigdatavicky')
    blob = bucket.blob(file_name)
    if blob.exists():
        gcs_df = spark.read.option("header", "false").option("delimiter", ",").csv(gcs_path)
        gcs_df.show(2)
    else:
        print("File is not available")

main()