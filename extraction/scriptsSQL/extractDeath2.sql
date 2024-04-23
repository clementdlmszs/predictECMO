SELECT 
	TOP 1 ptc.dischargeDisposition as is_deceased, ptc.outTime as outTime
FROM
	CISReportingDB.dar.PatientCensus ptc
WHERE
	ptc.encounterId = :encounterId
ORDER BY
	ptc.outTime DESC 