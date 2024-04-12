SELECT 
    try_cast(replace(R.terseForm, ',', '.') as decimal) HR,
    DATEDIFF(MINUTE, :installation_date, R.chartTime) as temps
FROM 
    [CISReportingDB].[dbo].[PtAssessment] R
WHERE 
	R.encounterId = :encounterId
    AND R.attributeId IN (616, 619, 617, 618)
    AND R.chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
    --AND try_cast(replace(R.terseForm, ',', '.') as decimal) > 0
ORDER BY
    temps