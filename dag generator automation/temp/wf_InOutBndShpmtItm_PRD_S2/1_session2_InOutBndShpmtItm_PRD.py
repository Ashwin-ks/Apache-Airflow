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
    with open (config.ENVIRONMENT_PATH+'m_InOutBndShpmtItm_00_ACQUIRE_PRD.sql', 'r') as oracleinput:
        line = oracleinput.read()
        lines = line.replace('%reqid',Requestnum).replace('%partno',Partnum)
    with open (config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'oracle_output1.sql', 'w') as oracleoutput:
        oracleoutput.write(lines)
    with open (config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'oracle_output1.sql', 'rU') as oracleoutput:
        oracleoutput = oracleoutput.readlines()
except IOError as e:
   print (e)
   raise


###Connect to Oracle database using db_utils & fetch records for processing
try:
    db_obj = MngDb().get_db_obj(config.v_aws_region, config.v_bucket_name, config.v_config_name)
    with open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'oracle_output1.sql', 'r') as foutsapbw:
        full_sql = foutsapbw.read()
except IOError as e:
   print (e)
   raise
except Exception as e:
   print (e)
   raise
print full_sql
v_select_op = db_obj.get_all(full_sql)
f = open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'recout1.txt','w')
os.chmod(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'recout1.txt',0o777)
with open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'recout1.txt', 'w') as recout:
     for row in v_select_op:
        o=csv.writer(recout,delimiter='|')
        o.writerow(row)


