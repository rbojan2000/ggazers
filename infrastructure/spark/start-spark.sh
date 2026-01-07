#!/bin/bash
if [ "$SPARK_WORKLOAD" == "master" ]; then
  exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master --host "$SPARK_LOCAL_IP"
elif [ "$SPARK_WORKLOAD" == "worker" ]; then
  exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker "$SPARK_MASTER"
else
  echo "Unknown SPARK_WORKLOAD: $SPARK_WORKLOAD"
  exit 1
fi