SELECT 
    valueNumber as debit,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    CISReportingDB.dar.PatientAssessment
WHERE 
	encounterId = :encounterId
    AND attributeId = 29094
    AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
    --AND valueNumber > 0
    AND clinicalUnitId = 3
ORDER BY
    temps