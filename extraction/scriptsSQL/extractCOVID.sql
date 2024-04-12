SELECT 
	CASE 
		WHEN 
			terseForm LIKE 'Epid_mie COVID-19'
		THEN 
			1
		ELSE 
			0
	END
    AS 'covid'
FROM 
    CISReportingDB.dbo.PtAssessment
WHERE 
	encounterId = :encounterId
    AND attributeId = 63830
    AND terseForm LIKE 'Epid_mie COVID-19'
    AND clinicalUnitId = 3