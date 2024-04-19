SELECT
    try_cast(replace(valueNumber, ',', '.') as float) weight,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps    
FROM 
    CISReportingDB.dbo.PtAssessment
WHERE
	encounterId = :encounterId
    AND interventionId = 4008
    --AND valueNumber > 0
ORDER BY
    temps