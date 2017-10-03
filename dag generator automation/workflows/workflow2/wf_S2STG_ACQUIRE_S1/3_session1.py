#!/usr/bin/env python2.7						
# -*- coding: utf-8 -*-
import csv
import os
import re
import sys
import cx_Oracle
import snowflake.connector
import config
from db_util.src.mng_db import MngDb
#os.system (cd /home)
os.system("chmod 777 /home/dev")
###Connect to SNOWFLAKE
RqstId = ''
try:
    with open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'reqlog_all.txt', 'r') as requestid:
        lines = requestid.readlines()
except IOError as e:
    print (e)
    raise
for line in lines:
    line = line.strip()
    RqstId = RqstId + ' ' +"'"+line+"'"
RqstId = RqstId.lstrip()
rid = RqstId.replace(" ",",")
rid = '('+rid+')'
ALL_REQ_ID = rid
print ALL_REQ_ID

rstatusRqstId=''
try:		
    with open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'reqlog_r.txt', 'r') as rstatuslog:
        rstatuslines = rstatuslog.readlines()
except IOError as e:
   print (e)
   raise 
for line in rstatuslines:
    rstatusRqstId = rstatusRqstId + ' ' +"'"+line.strip()+"'" 
rstatusRqstId = rstatusRqstId.lstrip()
rstatusid = rstatusRqstId.replace(" ",",")
rstatusid = '('+rstatusid+')'
						   
RqstSID1 = rstatusid
print RqstSID1

### Generate Sql
try:
    with open (config.INPUT_DIR+config.ORACLE_SQL_INPUTFILE, 'r') as oracleinput:
        line = oracleinput.read()
        lines = line.replace('%LOG_PSA_NAME',config.LOG_PSA_NAME).replace('%LOG_REQUEST_SYSTEM',config.LOG_REQUEST_SYSTEM).replace('%ALL_REQ_ID',ALL_REQ_ID).replace('%Rstatus_REQ_ID',RqstSID1)
    with open (config.ENVIRONMENT_PATH+config.ORACLE_SQL_OUTPUTFILE, 'w') as oracleoutput:
    	oracleoutput.write(lines)
    with open (config.ENVIRONMENT_PATH+config.ORACLE_SQL_OUTPUTFILE, 'rU') as oracleoutput:
        oracleoutput = oracleoutput.readlines()
        for line in oracleoutput:
            sql_new = lines
    with open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'sqlquery.sql', 'w') as fh: 
        fh.write(sql_new)  
except IOError as e:
   print (e)
   raise
print("connecting to oracle")
###Connect to Oracle database using db_utils & fetch records for processing
try:
    db_obj = MngDb().get_db_obj(config.v_aws_region,config.v_bucket_name,config.v_config_name)
    with open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'sqlquery.sql', 'r') as foutsapbw:
        full_sql = foutsapbw.read()
except IOError as e:
   print (e)
   raise
except Exception as e:
   print (e)
   raise
sqlCommands = full_sql.split('&&&')
print sqlCommands
for sql_command in sqlCommands:
   v_select_op = db_obj.get_all(sql_command)
print v_select_op
 
try:
    with open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'reqlog_recout.txt', 'w') as recout:   
        for row in v_select_op: 
            o=csv.writer(recout)
            o.writerow(row)
except IOError as e:
   print (e)
   raise
finally:
    recout.close()
recfh=open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'reqlog_recout.txt',"r")
lines=recfh.readlines()
partno=''
request=''
for x in lines:
     if len(x) <= 1:
        continue
     y = x.split(',')[4]
     z = x.split(',')[5]
     partno = partno + y + ' '
     request = request + "'"+z + "'"+' '
recfh.close()
partno = partno.replace(' ',',')
request = request.replace(' ',',')
PARTNO = partno[:-1]
REQUEST = request[:-1]


### Construct parameter file
paramout = open(config.ENVIRONMENT_PATH+config.TABLE_NAME+'_'+'param_out.txt','w')
list = ['TABLE_NAME_XI','BATCH_GROUP','OutputFile_Name','BadFile_XI','ParamSrcTable','ParamTgtTable']
try:
    with open (config.INPUT_DIR+config.TABLE_NAME+'_'+'param_in.txt') as paramin:
        for line in paramin:
           if any(s in line for s in list):
               print ("yay!",line)
#              matchObj = re.match( r"(\$\$.*=)(.*=.*)",line,re.M|re.I)
               matchObj = re.match( r"(\$.*=)(.*=.*)",line,re.M|re.I)
               if matchObj:
                   output = matchObj.group(2)
                   paramout.write("%s\n" % output)
        if PARTNO != '':
            paramout.write("$PARTNO=%s\n" % PARTNO)
        if REQUEST != '':
            paramout.write("$REQUEST=%s\n" % REQUEST)
except IOError as e:
   print (e)
   raise
finally:
   paramout.close()
