SELECT 
    cth.CompletedBY AS [Completed by name],
    
    -- Count of unique Case + Task combinations for '3dplan'
    COUNT(DISTINCT CASE WHEN cth.Task = '3dplan' 
                        THEN CONCAT(ca.CaseNumber, '_', cth.Task) 
                   END) AS [Sum of 3dplan tasks],
                   
    -- Count of unique Case + Task combinations for '3dfin-exp'
    COUNT(DISTINCT CASE WHEN cth.Task = '3dfin-exp' 
                        THEN CONCAT(ca.CaseNumber, '_', cth.Task) 
                   END) AS [Sum of 3dfin-exp],
                   
    CAST(cth.completeDate AS DATE) AS [completedate]
FROM dbo.CaseTasksHistory AS cth
INNER JOIN dbo.cases AS ca
    ON ca.CaseID = cth.CaseID
WHERE cth.completeDate >= CAST(DATEADD(day, -7, GETDATE()) AS DATE)
  AND cth.completeDate < CAST(DATEADD(day, 1, GETDATE()) AS DATE)
  AND cth.Task IN ('3dplan', '3dfin-exp')
GROUP BY 
    cth.CompletedBY,
    CAST(cth.completeDate AS DATE)
ORDER BY 
    [completedate] ASC, 
    [Completed by name] ASC;