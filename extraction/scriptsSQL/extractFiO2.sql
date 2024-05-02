SELECT 
    try_cast(replace(terseForm, ',', '.') as float) FiO2,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    [CISReportingDB].[dbo].[PtAssessment]
WHERE 
	encounterId = :encounterId
    AND attributeId = 33449
	AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
ORDER BY
    temps