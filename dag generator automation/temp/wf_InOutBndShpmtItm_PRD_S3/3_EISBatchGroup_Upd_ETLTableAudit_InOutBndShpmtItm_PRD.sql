UPDATE EIS_T.A_ETL_TABLE_AUDIT A_ETLTblAudit
SET 
      A_ETLTblAudit.TBL_ETL_STATUS=SrcFields.Tbl_ETL_Status,
      A_ETLTblAudit.TBL_ETL_END_TMST=SrcFields.TBL_ETL_END_TMST,
      A_ETLTblAudit.REJECTED_ROWS_CNT=SrcFields.REJECTED_ROWS_CNT,
      A_ETLTblAudit.NBR_OF_WARNINGS=SrcFields.NBR_OF_WARNINGS,
      A_ETLTblAudit.NBR_OF_SOURCE_ROWS=SrcFields.Nbr_Of_Source_Rows,
      A_ETLTblAudit.NBR_OF_CHANGED_ROWS=SrcFields.Nbr_Of_Source_Rows
FROM 
(SELECT    
     ETL_TABLE_AUDIT_PK,
     'Completed'as Tbl_ETL_Status,
     Current_Timestamp(0) TBL_ETL_END_TMST,
     sev1.REJECTED_ROWS_CNT REJECTED_ROWS_CNT,
     sev2.NBR_OF_WARNINGS NBR_OF_WARNINGS,
     XI.Nbr_Of_Source_Rows Nbr_Of_Source_Rows
From    
     (Select    Count(*)  REJECTED_ROWS_CNT  
     from    eis_stg_t.ECH_ERROR_WARNINGS_REC
     Where    Exception_Severity=1 AND Audit_FK=(Select    ETL_TABLE_AUDIT_PK
     from    EIS_T.A_ETL_TABLE_AUDIT where table_name='VTTP_D_xi' and TBL_ETL_STATUS='In Progress')) Sev1,
     (Select    Count(*)  NBR_OF_WARNINGS
     from    eis_stg_t.ECH_ERROR_WARNINGS_REC 
     Where    Exception_Severity in(2,3) AND Audit_FK=(Select    ETL_TABLE_AUDIT_PK
     from    EIS_T.A_ETL_TABLE_AUDIT where table_name='VTTP_D_xi' and TBL_ETL_STATUS='In Progress')) Sev2,
     (Select    Count(*) Nbr_Of_Source_Rows  
     from    eis_stg_t.VTTP_D_xi  WHERE AUDITFK IN (Select  max(ETL_TABLE_AUDIT_PK)  
     from    EIS_T.A_ETL_TABLE_AUDIT 
     where     Table_Name='VTTP_D_xi' and Tbl_ETL_Status = 'In Progress' )) XI,
     (Select    *  
     from    EIS_T.A_ETL_TABLE_AUDIT 
     where    ETL_TABLE_AUDIT_PK  In
     (Select    Max(ETL_TABLE_AUDIT_PK) 
     from    EIS_T.A_ETL_TABLE_AUDIT 
     where    Table_Name='VTTP_D_xi' and Tbl_ETL_Status = 'In Progress' )) A) SrcFields
WHERE SrcFields.ETL_TABLE_AUDIT_PK=A_ETLTblAudit.ETL_TABLE_AUDIT_PK