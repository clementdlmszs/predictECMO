SELECT 
    try_cast(replace(terseForm, ',', '.') as float) Pplat,
    dictionaryPropName as dictPropName,
    DATEDIFF(MINUTE, :installation_date, chartTime) as temps
FROM 
    CISReportingDB.dar.PatientAssessment
WHERE 
	encounterId = :encounterId
    AND dictionaryPropName IN ('MeanAirwayPressInt.Pressure_in_cmH20MSmt', 'PIPInt.PIPMsmt', 'PressureSupportInt.PressureSupportMsmt', 'PeakAirwayPressInt.Pressure_in_cmH20MSmt')
    AND chartTime BETWEEN :installation_date AND DATEADD(HOUR, 4, :withdrawal_date)
ORDER BY
    temps