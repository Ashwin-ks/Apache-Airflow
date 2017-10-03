import subprocess

aws_cmd=["aws s3 rm s3://rev-logistics-raw-preprod/qa/FileLanding/FullLoad/MasterData/EDW/STG_TBL/lips_outb_d_xi_v_prd_t.txt.gz",
"aws s3 rm s3://rev-logistics-raw-preprod/qa/FileLanding/FullLoad/MasterData/EDW/STG_TBL/EB_OutDlvrySchdln_E.txt.gz"]
try:
    for cmd in aws_cmd:
        push=subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE)
        print push.returncode
        out,err=push.communicate()
        if out:
            print "success"
        else:
             print err
             continue
               
except:
    pass

finally:
    print "aws s3 rm commands have been executed"
