SELECT 
	igsTotal,
    igsMort
FROM 
	CISReportingDB.dbo.PMSI_IGSDetails
WHERE
	FicheId IN (SELECT 
			    	FicheId 
			    FROM 
			   		CISReportingDB.dbo.[PMSI_Data.OLD] pdo
			    WHERE 
			   		pdo.cisEncounterId = (SELECT 
			   	    						cisEncounterId 
			   	    					  FROM 
			   	    					  	CISReportingDB.dar.M_CisEncounter mce 
			   	    					  WHERE 
			   	    					  	mce.encounterId = :encounterId
			   	    					  )
			    )