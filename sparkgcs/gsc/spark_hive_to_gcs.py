from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import date
from google.cloud import storage

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
    hive_table="gcs.customer"

#getting current date
    curr_date =date.today().strftime("%m%d%y")
    file_name = "custs_"+curr_date
    gcs_path = "gs://bigdatavicky2/"+file_name

#service account to access gc with the private key
    path_to_private_key="/home/hduser/install/projectbigdata-395203-26f51bbb59ce.json"
    print(f"Hive table {hive_table} reading starts")
    df = spark.read.table(hive_table)
    print(f"Number of partition - {df.rdd.getNumPartitions()}")
    print(f"Hive table {hive_table} reading completed")
    print(f"Writing to GCS started .... ")
    df.repartition(2).write.mode("overwrite").csv(gcs_path)
    print(f"Hive to GCS write completed successfully")

main()

