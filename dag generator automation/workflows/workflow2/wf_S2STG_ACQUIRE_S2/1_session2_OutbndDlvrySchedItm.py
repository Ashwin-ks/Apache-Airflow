#!/usr/bin/env python2.7
import sys
import csv
import snowflake.connector
import os
import config
from db_util.src.mng_db import MngDb
f1 = open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'param_out.txt','r')
f1 = f1.readlines()
for line in f1:
    if '$REQUEST' in line:
        Requestnum=line.replace('$REQUEST=','')
        print Requestnum
    if '$PARTNO' in line:
        Partnum = line.replace('$PARTNO=','')
        Partnum = Partnum.strip()
        print Partnum

try:
    with open (config.INPUT_DIR+config.ORACLE_EXPORT_SQL, 'r') as oracleinput:
        line = oracleinput.read()
        lines = line.replace('%reqid',Requestnum).replace('%partno',Partnum)
    with open (config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+config.ORACLE_EXPORT_OUTPUTFILE, 'w') as oracleoutput:
        oracleoutput.write(lines)
    with open (config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+config.ORACLE_EXPORT_OUTPUTFILE, 'rU') as oracleoutput:
        oracleoutput = oracleoutput.readlines()
except IOError as e:
   print (e)
   raise


###Connect to Oracle database using db_utils & fetch records for processing
try:
    db_obj = MngDb().get_db_obj(config.v_aws_region, config.v_bucket_name, config.v_config_name)
    with open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+config.ORACLE_EXPORT_OUTPUTFILE, 'r') as foutsapbw:
        full_sql = foutsapbw.read()
except IOError as e:
   print (e)
   raise
except Exception as e:
   print (e)
   raise
print full_sql
v_select_op = db_obj.get_all(full_sql)
with open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'recout1.txt', 'w') as recout:
     for row in v_select_op:
        o=csv.writer(recout,delimiter='|')
        o.writerow(row)


