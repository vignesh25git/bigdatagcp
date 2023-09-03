from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import date

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
    hdfs_path="hdfs://localhost:54310/user/hduser/datatotransfer/"

#getting current date
    curr_date =date.today().strftime("%m%d%y")
    file_name = "custs_"+curr_date
    gcs_path = "gs://bigdatavicky1/"
    hdfs_path = hdfs_path+file_name

#service account to access gc with the private key
    path_to_private_key="/home/hduser/install/projectbigdata-395203-26f51bbb59ce.json"
#check the file existence in the google cloud storage using google cloud storage api
    if os.path.exists(hdfs_path):
        custstructtype1 = StructType([StructField("id", IntegerType(), False),
                                      StructField("custfname", StringType(), False),
                                      StructField("custlname", StringType(), True),
                                      StructField("custage", ShortType(), True),
                                      StructField("custprofession", StringType(), True)])

        hdfs_df = spark.read.option("header", "false").\
            option("delimiter", ",").\
            option("schema","custstructtype1").\
            csv(hdfs_path)

        hdfs_df.show(2)
        hdfs_df.coalesce(1).write.csv(gcs_path+file_name)
        print(f'Todays dated file {file_name} has been transferred to GCS Successfully')
    else:
        print("File is not available in HDFS")

main()