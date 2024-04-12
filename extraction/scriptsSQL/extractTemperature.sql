SELECT 
	try_cast(replace(terseForm, ',', '.') as float) AS temperature,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
	[CISReportingDB].[dar].PatientAssessment
WHERE
	encounterId = :encounterId
	AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
	AND attributeId IN (8715,8717)
ORDER BY
    temps