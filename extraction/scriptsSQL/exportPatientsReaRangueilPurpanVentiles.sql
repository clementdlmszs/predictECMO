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
    P2.clinicalUnitId,
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
        MAX(chartTime) AS withdrawal_date,
        clinicalUnitId
    FROM 
        CISReportingDB.dar.PatientTreatment P
    WHERE 
        P.clinicalUnitId IN (3,9)
        and interventionId = 98026
        and YEAR(P.chartTime) >= 2020
        and YEAR(P.chartTime) < 2024
        AND P.patientAge >= 18
    GROUP BY
        encounterId,
        patientEncounterNumber,
        patientLifetimeNumber,
        patientAge,
        patientGender,
        patientFullName,
        patientDateOfBirth,
        clinicalUnitId
) P2
LEFT JOIN entree_et_sortie ON P2.encounterId = entree_et_sortie.encounterId
WHERE 
	DATEDIFF(   
			HH,
            P2.installation_date,
            P2.withdrawal_date
            ) > (24*3)
ORDER BY
	encounterId