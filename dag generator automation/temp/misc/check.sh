#!/usr/bin/env bash
DAGID=$1
DATE=$2
trigger_dag1=$3
trigger_dag2=$4
exec_count=$(python2.7 test.py -id ${DAGID} -date ${DATE})
c=`echo "$exec_count" |tail -n1`
echo "$c"
if [ "$c" -eq 1 ]; then
  echo "airflow trigger_dag ${trigger_dag1}"
  echo "airflow trigger_dag ${trigger_dag1}"
elif [ "$c" -eq 2 ]; then
  echo "airflow trigger_dag ${trigger_dag2}"  
else
  echo "Do Nothing"  
fi
