#!/usr/bin/env bash
#=================================================================================================
# Script Name : generate_dag_s2stg.sh
# Purpose     : This script will produce dag and push it to S3.
# Usage       : daggen.sh <s3 bucket> <env>
#               Ex: sh generate_dag.sh edf-infrabootstrap-preprod dev
#=================================================================================================
INFRABUCKET=$1
ENV=$2

echo "$INFRABUCKET $ENV"
#cd ../tools/dag-generator-s2stg/
echo "Be Steady! S2STG DAG's are being generated."
#/opt/python27/bin/python2.7 daggen.py --df dag_file.txt -w ../../../code/workflows -s s3://${INFRABUCKET}/airflow/revlogistics/${ENV}/var/workflows -o EDF -d generated_dags -p wf_
/usr/bin/python ../daggen_s2stg.py -w ../workflows/InOutBndShpmtItm/ -s s3://${INFRABUCKET}/airflow/revlogistics/${ENV}/var/s2stg_workflows/ -o EDF -d generated_dags -p wf_
if [ $? -eq 0 ]; then
  echo "S2STG DAG's generated successfully!"
else
  echo "S2STG DAG's creation failed!"
  exit 1
fi

#aws s3 sync generated_dags_s2stg s3://${INFRABUCKET}/airflow/revlogistics/${ENV}/var/dags

#if [ $? -eq 0 ]; then
  #echo "Generated S2STG DAG's moved successfully to s3!"
#else
  #echo "Movement of generated S2STG DAG's to s3 failed!"
  #exit 1
#fi

cd ../../../
