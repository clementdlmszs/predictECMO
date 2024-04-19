SELECT
    try_cast(replace(terseForm, ',', '.') as float) AS pas_i,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    [CISReportingDB].[dbo].[PtAssessment]
WHERE 
	encounterId = :encounterId
    AND attributeId IN (12538, 12527)
	AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
    --AND try_cast(replace(terseForm, ',', '.') as decimal) > 0
ORDER BY
    temps