#!/bin/bash
source /etc/zshrc
loadts=$(date '+%Y-%m-%d-%H-%M-%S')
echo "`date` gcloud hive ETL script is started"
echo "`date` gcloud hive ETL script is started" &> /tmp/gcp_hive_schedule_$loadts.log
gcloud dataproc jobs submit hive --cluster=cluster-iz-dplr --region us-east1 -e "create table if not exists transactions (txnno INT, txndate STRING, custno INT, amount DOUBLE,category string, product STRING, city STRING, state STRING, spendby STRING) row format delimited fields terminated by ',' stored as textfile" &> /tmp/gcp_hive_schedule_$loadts.log
if [ $? -ne 0 ]
then
echo "`date` error occured in the hive table creation part of the EL" >> /tmp/gcp_hive_schedule_$loadts.log
else
echo "`date` hive table creation part of the EL is completed successfully" >> /tmp/gcp_hive_schedule_$loadts.log
fi

#in onprem I was calling hive queries like this “hive –e "load data inpath '/user/ashfaqalamlearning/project/txns' overwrite into table transactions" “
gcloud dataproc jobs submit hive --cluster=cluster-iz-dplr --region us-east1 -e "load data inpath '/user/vigneshproject/project/txns' overwrite into table transactions" >> /tmp/gcp_hive_schedule_$loadts.log
if [ $? -ne 0 ]
then
echo "`date` error occured in the hive table load part of the EL" >> /tmp/gcp_hive_schedule_$loadts.log
else
echo "`date` hive table load part of the EL is completed successfully" >> /tmp/gcp_hive_schedule_$loadts.log
fi

loaddt=$(date '+%Y-%m-%d')

gcloud dataproc jobs submit hive --cluster=cluster-iz-dplr --region us-east1 --file=hdfs:///user/vigneshproject/project/cust_etl.hql --continue-on-failure --params=load_dt=$loaddt >> /tmp/gcp_hive_schedule_$loadts.log
if [ $? -ne 0 ]
then
echo "`date` error occured in the hive table creation part of the EL" >> /tmp/gcp_hive_schedule_$loadts.log
else
echo "`date` hive table creation part of the EL is completed successfully" >> /tmp/gcp_hive_schedule_$loadts.log
fi
echo "`date` gcloud hive ETL script is completed" >> /tmp/gcp_hive_schedule_$loadts.log