SELECT 
    try_cast(replace(terseForm, ',', '.') as float) Sp02,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    [CISReportingDB].[dbo].[PtAssessment]
WHERE 
	encounterId = :encounterId
    AND attributeId IN (1163, 1166, 1164, 1165)
	AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
    --AND try_cast(replace(terseForm, ',', '.') as decimal) > 0
ORDER BY
    temps