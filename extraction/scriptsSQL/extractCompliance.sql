SELECT 
    try_cast(replace(terseForm, ',', '.') as float) Compliance,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    CISReportingDB.dar.PatientAssessment
WHERE 
	encounterId = :encounterId
    AND dictionaryPropName = 'complianceDynamicRespInt.lungDynamicComplianceMsmt'
    AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
ORDER BY
    temps