SELECT 
    CASE 
        WHEN
            COUNT(case verboseForm when 'Veino-artérielle' then 1 else null end) >= COUNT(case verboseForm when 'Veino-veineuse' then 1 else null end)
        THEN 
            1
        ELSE
            0
    END
    AS ecmo_type
FROM 
    CISReportingDB.dar.PatientAssessment
WHERE 
	encounterId = :encounterId
	AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
    AND clinicalUnitId = 3
    AND attributeId = 5027