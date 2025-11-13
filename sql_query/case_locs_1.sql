/* Product Categories */
WITH ProductCategories AS
(
    SELECT 
        ca.CaseID,
        ca.CaseNumber,
        pr.Category
    FROM dbo.Cases AS ca
    LEFT JOIN dbo.Caseproducts AS cp
        ON ca.CaseID = cp.CaseID
    LEFT JOIN dbo.Products AS pr
        ON cp.ProductID = pr.ProductID
    WHERE 
        ca.Status = 'In Production'
        AND pr.Category IS NOT NULL
        AND pr.Category IN ('Metal', 'Clear', 'Wire Bending', 'Marpe', 'Hybrid', 'E2 Expanders', 'Lab to Lab', 'Airway')
),
/* Case Location – One Row Per CaseNumber (Unique) */
RankedCases AS
(
    SELECT
        ca.CaseNumber as [Case Number],
        ca.PanNumber as [Pan Number],
        ct.Task as [Last Task Completed],
        ct.CompleteDate as [Last Scan Time],
        pc.Category as [Category],
        CAST(ca.ShipDate AS DATE) AS [Ship Date],
        ca.LastLocationID,
        cll.[Description] AS [Last Location],
        ca.[Status],
        ca.LocalDelivery,
        ROW_NUMBER() OVER (
            PARTITION BY ca.CaseNumber
            ORDER BY
                CASE WHEN ct.CompleteDate IS NULL THEN 1 ELSE 0 END,
                ct.CompleteDate DESC,
                ct.CaseID DESC
        ) AS rn
    FROM dbo.Cases AS ca
    INNER JOIN dbo.CaseTasks AS ct 
        ON ca.CaseID = ct.CaseID
    LEFT JOIN dbo.CaseLogLocations AS cll 
        ON ca.LastLocationID = cll.ID
    LEFT JOIN ProductCategories AS pc
        ON ca.CaseID = pc.CaseID
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
    [LocalDelivery]
FROM RankedCases
WHERE rn = 1
ORDER BY [Case Number];