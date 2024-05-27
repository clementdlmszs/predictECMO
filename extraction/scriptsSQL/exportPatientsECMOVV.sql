WITH entree_et_sortie AS 
(
	SELECT
		encounterId,
		MIN(intime) as entree,
		MAX(outTime) as sortie
	FROM 
		CISReportingDB.dbo.ptCensus
	GROUP BY encounterId
)
SELECT
	P2.encounterId,
	P2.patientLifetimeNumber,
    P2.patientEncounterNumber,
    P2.patientFullName,
    P2.patientAge,
    P2.patientGender,
    P2.patientDateOfBirth,
    P2.installation_date,
    P2.withdrawal_date,
    entree,
    sortie
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
LEFT JOIN entree_et_sortie ON P2.encounterId = entree_et_sortie.encounterId
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
	            COUNT(case when verboseForm LIKE 'Veino-art%rielle' then 1 else null end) >= COUNT(case when verboseForm LIKE 'Veino-veineuse' then 1 else null end)
	        THEN 
	            1
	        ELSE
	            0
	    END
	    AS ecmo_type
	FROM 
	    CISReportingDB.dar.PatientAssessment PA
	WHERE 
	    clinicalUnitId = 3
	    AND attributeId = 5027
	    AND PA.encounterId = P2.encounterId
	    AND chartTime BETWEEN P2.installation_date AND DATEADD(HOUR, 4, P2.withdrawal_date)
	) = 0
ORDER BY
	P2.encounterId