INSERT INTO ECH_ERROR_WARNINGS_REC
(
     IDENTIFYING_TABLE_NAME,
     IDENTIFYING_COL_NAME,
     IDENTIFYING_COL_VALUE,
     EXCEPTION_COL_NAME,
     EXCEPTION_COL_VALUE,
     EXCEPTION_DESCRIPTION,
     EXCEPTION_SEVERITY,
     AUDIT_FK,
     LOAD_DATE
)
SELECT
     'VTTP_D_xi' as IDENTIFYING_TABLE_NAME,
     'TKNUM'||'TPNUM' as IDENTIFYING_COL_NAME,
     SRC.TKNUM|| ', '||SRC.TPNUM	 AS IDENTIFYING_COL_VALUE,
     'TPLST' as EXCEPTION_COL_NAME,
     SRC.TPLST as EXCEPTION_COL_VALUE,
     'TrnsprtnPlngPtCd not found in TrnsprtnPlngPtTrfcConfig OR REGION not found in ZTLPMM_LOG_FLTR_XI 'as EXCEPTION_DESCRIPTION,
     1 as EXCEPTION_SEVERITY,
     B.ETL_TABLE_AUDIT_PK AS AUDIT_FK,
     Current_Date as LOAD_DATE
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
     (TPPTC.TrfcCd IS NULL OR LENGTH(LTRIM(RTRIM(TPPTC.TrfcCd)))=0 OR 
     ZNK.RGN IS NULL OR LENGTH(LTRIM(RTRIM(ZNK.RGN)))=0);