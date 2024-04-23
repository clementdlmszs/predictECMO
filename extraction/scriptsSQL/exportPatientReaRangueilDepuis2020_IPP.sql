SELECT
	DISTINCT patientLifetimeNumber
FROM
	CISReportingDB.dar.PatientCensus pc
WHERE
	clinicalUnitId = 3
	and YEAR(inTime) >= 2020
	and YEAR(inTime) < 2024
	and patientAge >= 18
	and patientLifetimeNumber LIKE '0________'