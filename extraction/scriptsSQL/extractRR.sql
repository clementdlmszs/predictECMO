SELECT 
	try_cast(replace(terseForm, ',', '.') as float) AS RR,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    [CISReportingDB].[dbo].[PtAssessment]
WHERE 
	encounterId = :encounterId
    AND attributeId IN (10112, 42968)
	AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
    --AND try_cast(replace(terseForm, ',', '.') as decimal) > 0
ORDER BY
    temps