SELECT 
    try_cast(replace(terseForm, ',', '.') as float) VolumeCourant,
    dictionaryPropName as dictPropName,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    CISReportingDB.dar.PatientAssessment
WHERE 
	encounterId = :encounterId
    AND dictionaryPropName IN ('exhaledTidalVolumePatientInt.TidalVolumeMsmt', 'exhaledTidalVolumePatientInt.TidalVolumeMsmt.ptPeds', 'TidalVolActualInt.TidalVolumeMsmt', 'TidalVolActualInt.TidalVolumeMsmt.ptPeds', 'TidalVolumeInt.TidalVolumeMsmt', 'TidalVolumeInt.TidalVolumeMsmt.ptPeds')
    AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
ORDER BY
    temps