SELECT 
(
	CASE WHEN  
	(
	    CASE WHEN EXISTS ( 
	        SELECT 
	            valueDateTime
	        FROM 
	            CISReportingDB.dbo.PtDemographic
	        WHERE
	            attributeId = 23893 -- Corresponds to "Patient : Date de décès.Date"
				AND encounterId = :encounterId				
	    )
	    THEN 1
	    ELSE 0
	    END	
	+
	    CASE WHEN 
	    ( 
	        SELECT 
	            TOP 1 LOWER(verboseForm)
	        FROM 
	            CISReportingDB.dbo.PtDemographic
	        WHERE 
	            attributeId =  28160 -- Corresponds to ".Mode de sortie (liste).Mode de sortie"
				AND encounterId = :encounterId
	        ORDER BY 
	            utcChartTime DESC
	    ) LIKE '%deces%'
	        THEN 1
	        ELSE 0
	        END
	+
		CASE WHEN 
		(
			SELECT
				TOP 1 LOWER(ptc.dischargeDisposition)
			FROM
				CISReportingDB.dbo.PtCensus ptc
			WHERE
				encounterId = :encounterId
			ORDER BY
				ptc.outTime DESC 
		) LIKE '%deces%'
			THEN 1
			ELSE 0
			END 
	) > 0
	THEN 
	1
	ELSE 
	0
	END
)