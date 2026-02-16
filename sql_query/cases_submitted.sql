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
  AND CAST(ca.DateIn AS DATE) >= CAST(DATEADD(day, -7, GETDATE()) AS DATE)
ORDER BY ca.ShipDate;
