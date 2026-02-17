SELECT
    ca.Status AS TypeCount,
    CAST(ca.ShipDate AS DATE) AS ShipDate,
    ca.PanNumber
FROM dbo.Cases ca
WHERE ca.Status IN ('In Production', 'Invoiced')
  AND ca.ShipDate IS NOT NULL
  AND ca.ShipDate > CAST(DATEADD(day, -5, GETDATE()) AS DATE)
  AND ca.ShipDate < CAST(DATEADD(day, 11, GETDATE()) AS DATE)
ORDER BY ShipDate DESC;
