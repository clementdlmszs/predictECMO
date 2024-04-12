SELECT
    D.valueNumber weight,
    DATEDIFF(MINUTE, :installation_date, D.chartTime) as temps   
FROM 
    CISReportingDB.dbo.ptDemographic D
WHERE
	D.encounterId = :encounterId
    AND D.interventionId IN (2908, 19176, 4258)
    --AND D.valueNumber > 0
ORDER BY
    temps