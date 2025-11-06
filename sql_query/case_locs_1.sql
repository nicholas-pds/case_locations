/*  Case Location – One Row Per CaseNumber (Unique)  */
WITH RankedCases AS
(
    SELECT
        ca.CaseNumber,
        ca.PanNumber,
        ct.Task,
        CAST(ct.CompleteDate AS DATE) AS CompleteDate,
        CAST(ca.ShipDate     AS DATE) AS ShipDate,
        ca.LastLocationID,
        cll.Description AS LocationDescription,
        ca.Status,
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
    CaseNumber,
    PanNumber,
    Task,
    CompleteDate,
    ShipDate,
    LocationDescription,
    [Status],
    LocalDelivery
FROM RankedCases
WHERE rn = 1
ORDER BY CaseNumber;