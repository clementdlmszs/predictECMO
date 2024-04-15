SELECT 
(
	CASE WHEN  
	(
	    CASE WHEN EXISTS ( 
	        SELECT 
	            valueDateTime
	        FROM 
	            demographicSimplifie
	        WHERE
	            attributeId = 23893 -- Corresponds to "Patient : Date de décès.Date"
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
	            demographicSimplifie
	        WHERE 
	            attributeId =  28160 -- Corresponds to ".Mode de sortie (liste).Mode de sortie"
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
				ptc.encounterId = :encounterId
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