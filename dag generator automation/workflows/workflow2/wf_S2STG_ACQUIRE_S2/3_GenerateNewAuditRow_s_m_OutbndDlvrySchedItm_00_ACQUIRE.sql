INSERT INTO {{SCHEMA_T}}.A_ETL_TABLE_AUDIT
( 
     TABLE_NAME, 
     Table_Type,
     Table_Extract_Type, 
     Tbl_ETL_Start_Tmst, 
     Tbl_ETL_Status
) 
SELECT 
	 InsVals.TABLE_NAME,
	 InsVals.Table_Type,
	 InsVals.Table_Extract_Type,
	 InsVals.Tbl_ETL_Start_Tmst,
	 InsVals.Tbl_ETL_Status
	 FROM 
	 (SELECT
		'{{PARAMTGTTABLE}}' AS TABLE_NAME, 
		'STAGING' AS Table_Type, 
		'INCREMENTAL' AS Table_Extract_Type, 
		CURRENT_TIMESTAMP(0) Tbl_ETL_Start_Tmst,
		'In Progress' AS Tbl_ETL_Status) InsVals,
	(SELECT COUNT(1) AS COUNT_EXIST_AUDIT
	 FROM {{SCHEMA_T}}.A_ETL_TABLE_AUDIT 
	 WHERE 
		 TABLE_NAME='{{PARAMTGTTABLE}}' AND Tbl_ETL_Status='In Progress') CurrentStatusAudit,
	(SELECT COUNT(1) AS COUNT_EXIST_SOURCE
	 FROM {{SCHEMA_STG}}.{{PARAMSRCTABLE}})CurrentStatusSource
	WHERE
	     CurrentStatusAudit.COUNT_EXIST_AUDIT=0 and
		 CurrentStatusSource.COUNT_EXIST_SOURCE>0;	 	 