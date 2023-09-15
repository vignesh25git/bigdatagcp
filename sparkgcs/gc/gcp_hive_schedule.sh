#!/bin/bash
source /home/hduser/.bashrc
echo "`date` gcloud hive ETL script is started"
echo "`date` gcloud hive ETL script is started" &> /tmp/gcp_hive_schedule.log
gcloud dataproc jobs submit hive --cluster=cluster-d0914 --region us-central1 -e "create table if not exists transactions (txnno INT, txndate STRING, custno INT, amount DOUBLE,category string, product STRING, city STRING, state STRING, spendby STRING) row format delimited fields terminated by ',' stored as textfile" &>> /tmp/gcp_hive_schedule.log
if [ $? -ne 0 ]
then
echo "`date` error occured in the hive table creation part of the EL" &>> /tmp/gcp_hive_schedule.log
else
echo "`date` hive table creation part of the EL is completed successfully" &>> /tmp/gcp_hive_schedule.log
fi
gcloud dataproc jobs submit hive --cluster=cluster-d0914 --region us-central1 -e "load data inpath
'/user/vickyonline1983/project/txns' overwrite into table transactions" &>> /tmp/gcp_hive_schedule.log
if [ $? -ne 0 ]
then
echo "`date` error occured in the hive table load part of the EL" &>> /tmp/gcp_hive_schedule.log
else
echo "`date` hive table load part of the EL is completed successfully" &>> /tmp/gcp_hive_schedule.log
fi
loaddt=$(date '+%Y-%m-%d')
gcloud dataproc jobs submit hive --cluster=cluster-d0914 --region us-central1 --file=hdfs:///user/vickyonline1983/project/cust_etl.hql --continue-on-failure --params=load_dt=$loaddt &>> /tmp/gcp_hive_schedule.log
if [ $? -ne 0 ]
then
echo "`date` error occured in the hive table creation part of the EL" &>> /tmp/gcp_hive_schedule.log
else
echo "`date` hive table creation part of the EL is completed successfully" &>> /tmp/gcp_hive_schedule.log
fi
echo "`date` gcloud hive ETL script is completed" &>> /tmp/gcp_hive_schedule.log
echo "Completed"
