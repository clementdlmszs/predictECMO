SELECT 
(
	CASE WHEN 
	(
		SELECT
			TOP 1 LOWER(ptc.dischargeDisposition)
		FROM
			CISReportingDB.dbo.PtCensus ptc
		WHERE
			ptc.encounterId = :encounterId
		ORDER BY
			ptc.outTime DESC 
	) LIKE '%deces%'
		THEN 1
		ELSE 0
		END 
)
