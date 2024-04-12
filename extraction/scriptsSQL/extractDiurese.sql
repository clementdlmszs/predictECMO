SELECT 
    try_cast(replace(baseHourTotal, ',', '.') as float) diurese_heure,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
	CISReportingDB.dar.PatientTotalBalance ptb 
WHERE
	encounterId = :encounterId
	AND interventionID = 1395
	AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
ORDER BY
   temps