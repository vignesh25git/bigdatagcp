from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
# define spark configuration object
    spark = SparkSession.builder\
                .appName("GCP GCS Hive Read/Write") \
                .enableHiveSupport()\
                .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sc=spark.sparkContext
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    print("Use Spark Application to Read csv data from cloud GCS and get a DF created with the GCS data in the on prem,"
          "convert csv to json in the on prem DF and store the json into new cloud GCS location")
    print("Hive to GCS to hive starts here")
    custstructtype1 = StructType([StructField("id", IntegerType(), False),
                                  StructField("custfname", StringType(), False),
                                  StructField("custlname", StringType(), True),
                                  StructField("custage", ShortType(), True),
                                  StructField("custprofession", StringType(), True)])

    gcs_df = spark.read.csv("gs://bigdatavicky2/dataset/custs",mode='dropmalformed',schema=custstructtype1)
    gcs_df.show(10)
    print("GCS Read Completed Successfully")
    gcs_df.write.mode("overwrite").partitionBy("custage").saveAsTable("default.cust_info_gcs")
    print("GCS to hive table load Completed Successfully")
    print("Hive to GCS usecase starts here")
    gcs_df=spark.read.table("default.cust_info_gcs")
    curts = spark.createDataFrame([1], IntegerType()).withColumn("curts",current_timestamp()).select(date_format(col("curts"), "yyyyMMddHHmmSS")).first()[0]
    print(curts)
    gcs_df.repartition(2).write.json("gs://bigdatavicky2/dataset/cust_output_json_"+curts)
    print("gcs Write Completed Successfully")
    print("Hive to GCS usecase starts here")
    gcs_df=spark.read.table("default.cust_info_gcs")
    curts = spark.createDataFrame([1], IntegerType()).withColumn("curts",
    current_timestamp()).select(date_format(col("curts"), "yyyyMMddHHmmSS")).first()[0]
    print(curts)
    gcs_df.repartition(2).write.mode("overwrite").csv("gs://bigdatavicky2/dataset/cust_csv")
    print("gcs Write Completed Successfully")

main()