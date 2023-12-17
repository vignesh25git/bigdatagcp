#!/bin/bash
#source /home/hduser/.bashrc
loadts=$(date '+%Y-%m-%d-%H-%M-%S')
echo "`date` gcloud hive ETL script is started"
echo "`date` gcloud hive ETL script is started" &> /tmp/gcp_hive_schedule.log
echo "`date` error occured in the hive table creation part of the EL" >> /tmp/gcp_hive_schedule.log
echo "`date` hive table creation part of the EL is completed successfully" >> /tmp/gcp_hive_schedule.log
