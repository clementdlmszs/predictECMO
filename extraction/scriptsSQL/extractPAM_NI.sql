SELECT
    try_cast(replace(terseForm, ',', '.') as decimal) AS pam_ni,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    [CISReportingDB].[dbo].[PtAssessment]
WHERE 
    encounterId = :encounterId
    AND attributeId = 11820
	AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
    --AND try_cast(replace(terseForm, ',', '.') as decimal) > 0
ORDER BY
    temps