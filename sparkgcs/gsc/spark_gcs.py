from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
# define spark configuration object
    spark = SparkSession.builder\
                        .appName("HDFS - GCP GCS Read/Write") \
                        .config("spark.jars","/home/hduser/install/gcs-connector-hadoop2-2.2.7.jar")\
                        .enableHiveSupport()\
                        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sc=spark.sparkContext
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    conf.set("google.cloud.auth.service.account.json.keyfile","/home/hduser/install/projectbigdata-395203-26f51bbb59ce.json")
    print("Data Transfer between HDFS to GCS and Vice versa")
    hdfs_df=spark.read.csv("hdfs://localhost:54310/user/hduser/datatotransfer/")
    print("HDFS Read Completed Successfully")
    hdfscnt=hdfs_df.count()
    curts = spark.createDataFrame([1], IntegerType()).withColumn("curts", current_timestamp()).select(date_format(col("curts"),
    "yyyyMMddHHmmSS")).first()[0]
    print(curts)
    hdfs_df.coalesce(1).write.csv("gs://bigdatavicky/custdata_"+curts)
    gcs_df = spark.read.option("header", "false").option("delimiter", ",")\
                .csv("gs://bigdatavicky/custdata_"+curts)
    gcs_df.show(2)
    gcscnt=gcs_df.count()
    if (hdfscnt==gcscnt):
        print("gcs Write Completed Successfully including Data Quality/Reconcilation check completed (equivalent to sqoop --validate)")
    else:
        print("gcs Write Issue")

main()