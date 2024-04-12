SELECT
	*
FROM 
(
	SELECT
	    encounterId,
	    patientLifetimeNumber,
	    patientEncounterNumber,
	    patientFullName,
	    patientAge,
	    patientGender,
	    patientDateOfBirth,
	    MIN(chartTime) AS installation_date,
	    MAX(chartTime) AS withdrawal_date
	FROM 
	    CISReportingDB.dar.PatientAssessment P
	WHERE 
	    P.attributeId = 29094 -- = "Débit d'ECMO"
	    AND P.clinicalUnitId = 3 -- = "Réanimation Rangueil"
	    AND P.patientAge >= 18
	GROUP BY
	    encounterId,
	    patientEncounterNumber,
	    patientLifetimeNumber,
	    patientAge,
	    patientGender,
	    patientFullName,
	    patientDateOfBirth 
) P2
WHERE 
	DATEDIFF(   
				HH,
                P2.installation_date,
                P2.withdrawal_date
            ) > 24
	AND 
	(SELECT 
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
		chartTime BETWEEN P2.installation_date AND DATEADD(HOUR, 4, P2.withdrawal_date)
	    AND clinicalUnitId = 3
	    AND attributeId = 5027
	) = 0
ORDER BY
	encounterId