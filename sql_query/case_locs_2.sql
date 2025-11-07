/*  Case Location – One Row Per CaseNumber (Unique)  */
WITH RankedCases AS
(
    SELECT
        ca.CaseNumber as [Case Number],
        ca.PanNumber as [Pan Number],
        ct.Task as [Last Task Completed],
        ct.CompleteDate as [Last Scan Time],
        ct.Department as [Category],
        CAST(ca.ShipDate     AS DATE) AS [Ship Date],
        ca.LastLocationID,
        cll.Description AS [Last Location],
        ca.[Status],
        ca.LocalDelivery,
        ROW_NUMBER() OVER (
            PARTITION BY ca.CaseNumber
            ORDER BY
                CASE WHEN ct.CompleteDate IS NULL THEN 1 ELSE 0 END,  -- completed first
                ct.CompleteDate DESC,                                -- newest date
                ct.CaseID DESC                                        -- tie-breaker (or use ct.CaseTaskID if exists)
        ) AS rn
    FROM dbo.Cases AS ca
    INNER JOIN dbo.CaseTasks AS ct 
        ON ca.CaseID = ct.CaseID
    LEFT JOIN dbo.CaseLogLocations AS cll 
        ON ca.LastLocationID = cll.ID
    WHERE ca.Status = 'In Production'
)
SELECT
    [Case Number],
    [Pan Number],
    [Ship Date],
    [Status],
    [Category],
    [Last Location],
    [Last Task Completed],
    [Last Scan Time],
    LocalDelivery
FROM RankedCases
WHERE rn = 1
ORDER BY [Case Number];