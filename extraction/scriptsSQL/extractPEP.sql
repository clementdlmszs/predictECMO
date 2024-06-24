SELECT 
    try_cast(replace(terseForm, ',', '.') as float) PEP,
    dictionaryPropName as dictPropName,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    CISReportingDB.dar.PatientAssessment
WHERE 
	encounterId = :encounterId
    AND dictionaryPropName IN ('PEEPInt.PEEPMsmt', 'PEP_reglee.PEEPMsmt')
    AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
ORDER BY
    temps