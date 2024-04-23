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
        P.clinicalUnitId = 3
        and YEAR(P.chartTime) >= 2020
        and YEAR(P.chartTime) < 2024
        AND P.patientAge >= 18
        AND P.patientLifetimeNumber IN (SELECT
                DISTINCT patientLifetimeNumber as ltnb
            FROM
                [CISReportingDB].[dar].[PatientCensus] pc
            WHERE
                clinicalUnitId = 3
                and YEAR(inTime) >= 2020
                and YEAR(inTime) < 2024
                and patientAge >= 18
                and patientLifetimeNumber LIKE '0________'
            )
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