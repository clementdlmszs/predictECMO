WITH lifetimeNumbers AS
(
	SELECT
		DISTINCT patientLifetimeNumber
	FROM
		CISReportingDB.dar.PatientTreatment
	WHERE
		clinicalUnitId IN (3,9)
		and interventionId = 98026
		and patientAge >= 18
)
SELECT
	DISTINCT patientLifetimeNumber
FROM	
	CISReportingDB.dar.PatientCensus pc
WHERE
    YEAR(inTime) >= 2020
	and YEAR(inTime) < 2024
	and patientLifetimeNumber IN (SELECT patientLifetimeNumber FROM lifetimeNumbers)
	and patientLifetimeNumber LIKE '0________'