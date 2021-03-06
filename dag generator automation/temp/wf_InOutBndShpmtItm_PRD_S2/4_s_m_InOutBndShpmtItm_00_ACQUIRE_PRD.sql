INSERT INTO VTTP_D_xi
(    PARTNO,
     RECORD,
	 REQUEST_Id,
	 ROCANCEL,
	 TKNUM,
	 TPLST,
	 TPNUM,
	 VBELN,
	 RQSTSID ,
	 ROWEFFSTRTTMSTMP ,
	 RECORD_CHECKSUM,
	 DATAPAKID,
	 ETL_BtchGrpCd,
	 AuditFK
)	 
SELECT 
     SRC.PARTNO as PARTNO,
     SRC.RECORD as RECORD,
     SRC.REQUEST as REQUEST,
     SRC.ROCANCEL as ROCANCEL,
     SRC.TKNUM as TKNUM,
     SRC.TPLST as TPLST,
     SRC.TPNUM as TPNUM,
     SRC.VBELN as VBELN,
     TO_NUMBER(SRC.SID) as RQSTSID,
     TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP(0)) AS RowEffStrtTmstmp ,
     'NOTUSED' as Record_Checksum,
     TO_NUMBER(SRC.DATAPAKID) as DATAPAKID,
     DECODE(ZNK.RGN,
               'Asia Pacific',CONCAT(TPPTC.TrfcCd,'_ASIA')
               ,'United States',CONCAT(TPPTC.TrfcCd,'_USA')
               ,'EMEA',CONCAT(TPPTC.TrfcCd,'_EMEA')
               ,'Americas',CONCAT(TPPTC.TrfcCd,'_Americas')) AS ETL_BtchGrpCd,
	 B.ETL_TABLE_AUDIT_PK as AuditFK
FROM 
     EIS_STG_T.S_2LIS_08TRTLP_D SRC
LEFT OUTER JOIN
     EIS_STG_T.ZNK_TTDS_XF ZNK
ON SRC.TPLST=ZNK.TPLST
LEFT OUTER JOIN 
     eis_t.TrnsprtnPlngPtTrfcConfig TPPTC
ON RPAD(SRC.TPLST,5)=TPPTC.TrnsprtnPlngPtCd
LEFT OUTER JOIN
     (SELECT ETL_TABLE_AUDIT_PK FROM EIS_ETL.A_ETL_TABLE_AUDIT where table_name='VTTP_D_xi' AND TBL_ETL_STATUS='In Progress') B
ON 1=1
WHERE
     (TPPTC.TrfcCd IS NOT NULL OR LENGTH(LTRIM(RTRIM(TPPTC.TrfcCd)))>0 OR 
     ZNK.RGN IS NOT NULL OR LENGTH(LTRIM(RTRIM(ZNK.RGN)))>0)