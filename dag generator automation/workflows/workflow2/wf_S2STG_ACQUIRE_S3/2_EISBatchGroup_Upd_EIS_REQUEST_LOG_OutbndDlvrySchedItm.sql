UPDATE {{SCHEMA_T}}.EIS_REQUEST_LOG EISRequestLog
SET
    EISRequestLog.RQSTLoadStatCd=SrceFLDS.out_REQUEST_LOAD_STATUS
FROM
    (
    SELECT 
    C.RqstSID,
    C.REQUEST_ID,
    C.ETL_BtchGrpCd,
    C.TOTAL_DELTAS AS out_TOTAL_DELTAS,
    'R' AS out_STATUS,
    'C' AS out_REQUEST_LOAD_STATUS
    FROM
		(
		SELECT 
		A.RqstSID,
		A.REQUEST_ID,
		A.ETL_BtchGrpCd, 
		A.TOTAL_DELTAS
		FROM 
			(
			SELECT 
			X.RqstSID,
			X.REQUEST_ID,
			X.ETL_BtchGrpCd, 
			COUNT(X.RqstSID) TOTAL_DELTAS
			FROM
				{{SCHEMA_STG}}.{{PARAMTGTTABLE}} X 
			WHERE 
				AUDITFK IN 
					(
					SELECT ETL_TABLE_AUDIT_PK FROM {{SCHEMA_T}}.A_ETL_TABLE_AUDIT WHERE TABLE_NAME = '{{PARAMTGTTABLE}}' AND TBL_ETL_STATUS = 'In Progress'
					)
					GROUP BY  
					X.RqstSID,X.REQUEST_ID, X.ETL_BtchGrpCd
			) A 
			INNER JOIN 
				{{SCHEMA_T}}.EIS_REQUEST_LOG B 
				ON 
				A.RqstSID = B.RqstSID AND 
				B.RQSTLoadStatCd = 'R'
		) C
    ) SrceFLDS
	WHERE EISRequestLog.RqstSID=SrceFLDS.RQSTSID;
