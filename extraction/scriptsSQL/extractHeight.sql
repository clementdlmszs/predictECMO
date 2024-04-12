SELECT
    try_cast(replace(D.terseForm, ',', '.') as float) AS height,
    DATEDIFF(MINUTE, :installation_date, D.chartTime) as temps   
FROM 
    CISReportingDB.dbo.ptDemographic D
WHERE 
	encounterId = :encounterId
    AND attributeId = 15301