SELECT 
    try_cast(replace(terseForm, ',', '.') as float) HR,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    [CISReportingDB].[dbo].[PtAssessment]
WHERE 
	encounterId = :encounterId
    AND attributeId IN (616, 619, 617, 618)
    AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
    --AND try_cast(replace(R.terseForm, ',', '.') as decimal) > 0
ORDER BY
    temps