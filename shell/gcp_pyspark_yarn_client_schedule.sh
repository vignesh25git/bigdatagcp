#!/bin/bash
gcloud dataproc jobs submit pyspark --cluster=cluster-iz-dplr --region=us-east1  --zone us-east1-b --enable-component-gateway --properties="spark.driver.memory=2g","spark.executor.memory=2g","spark.executor.instances=4","spark.executor.cores=2","spark.submit.deployMode=client","spark.sql.shuffle.partitions=10","spark.shuffle.spill.compress=true" /home/vigneshvelusamy/project/Usecase4_GcpGcsReadWritehive_cloud.py
#spark-submit --master yarn --mode client….
if [ $? -ne 0 ]
then
echo "`date` error occured in the Pyspark job" > /tmp/gcp_pyspark_schedule.log
else
echo "`date` Pyspark job is completed successfully" > /tmp/gcp_pyspark_schedule.log
fi
echo "`date` gcloud pyspark ETL script is completed" >> /tmp/gcp_pyspark_schedule.log


