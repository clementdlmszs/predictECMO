WITH demographicSimplifie AS
(
	SELECT
		D.attributeId,
		D.valueDateTime,
		D.verboseForm,
		D.utcChartTime
	FROM
		CISReportingDB.dar.PtDemographic D
	WHERE
		D.encounterId = 258
		AND D.attributeId IN (23893,28160)
)
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
	   -- +
	   --     CASE WHEN 
	   --         P.code_destination = 65
	   --     THEN 1
	   --     ELSE NULL
	   --     END
	) > 0
	THEN 
	1
	ELSE 
	0
	END
)