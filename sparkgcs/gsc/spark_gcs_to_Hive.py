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
# hive tble name
    hdfs_path="hdfs://localhost:54310/user/hduser/datatotransfer/"
    hive_table="gcs.customer"

#getting current date
    curr_date =date.today().strftime("%m%d%y")
    file_name = "custs_"+curr_date
    hdfs_path = hdfs_path+file_name
    gcs_path = "gs://bigdatavicky1/"

#service account to access gc with the private key
    path_to_private_key="/home/hduser/install/projectbigdata-395203-26f51bbb59ce.json"
#check the file existence in the hdfs using os

    if os.path.exists(hdfs_path):
        custstructtype1 = StructType([StructField("id", IntegerType(), False),
                                      StructField("custfname", StringType(), False),
                                      StructField("custlname", StringType(), True),
                                      StructField("custage", ShortType(), True),
                                      StructField("custprofession", StringType(), True)])
        hdfs_df = spark.read.csv(hdfs_path,schema=custstructtype1,mode="dropmalformed")
        hdfs_df.printSchema()
        hdfs_df.show(3)
        print("HDFS read is successful")
        print("Writing to Google cloud is starting....")
        hdfs_df.coalesce(1).write.csv(gcs_path + file_name)
        print("write to gcs  is completed....")
    else:
        print("File is not available")

main()