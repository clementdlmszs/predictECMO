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
ORDER BY
	encounterId