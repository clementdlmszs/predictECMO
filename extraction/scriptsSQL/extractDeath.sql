SELECT 
    encounterId,
    MAX(dead) AS dead,
    MIN(inTime) AS 'reanimation_entry_date'
FROM 
(
    SELECT DISTINCT
        E.encounterId,
        CASE WHEN  
        (
            CASE WHEN EXISTS ( 
                SELECT 
                    D.valueDateTime
                FROM 
                    CISReportingDB.dar.PtDemographic AS D
                WHERE 
                    D.encounterId = E.encounterId
                    AND D.attributeId = 23893 -- Corresponds to "Patient : Date de décès.Date"
            )
            THEN 1
            ELSE NULL
            END	
        +
            CASE WHEN 
            ( 
                SELECT 
                    TOP 1 LOWER(D.verboseForm)
                FROM 
                    CISReportingDB.dar.PtDemographic AS D
                WHERE 
                    D.encounterId = E.encounterId
                    AND D.attributeId =  28160 -- Corresponds to ".Mode de sortie (liste).Mode de sortie"
                ORDER BY 
                    D.utcChartTime DESC
            ) LIKE '%deces%'
            THEN 1
            ELSE NULL
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
        END AS dead,
        C.inTime
    FROM 
        CISReportingDB.dbo.D_Encounter E
    LEFT JOIN 
        CISReportingDB.dar.PatientCensus as C
    ON 
        C.encounterId = E.encounterId
    --LEFT JOIN
    --    CISReportingDB.dbo.PMSI_Data.OLD as P
    --ON 
    --    P.cisEncounterId = C.cisEncounterId
    WHERE 
        C.clinicalUnitId = 3
) T
GROUP BY  
    encounterId