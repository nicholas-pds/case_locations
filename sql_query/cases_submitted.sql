SELECT
    ca.CaseNumber AS [Case Number],
    ca.PanNumber AS [Pan Number],
    CAST(ca.ShipDate AS DATE) AS [Ship Date],
    ca.Status,
    cll.[Description] AS [Last Location]
FROM dbo.Cases AS ca
LEFT JOIN dbo.CaseLogLocations AS cll
    ON ca.LastLocationID = cll.ID
WHERE ca.Status = 'Submitted'
ORDER BY ca.ShipDate;
