/* Product Categories – unchanged */
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
        AND pr.Category IN ('Metal', 'Clear', 'Wire Bending', 'Marpe', 'Hybrid', 'E2 Expanders', 'Lab to Lab', 'Airway')
),

/* Assign priority and pick only the highest-priority category per case */
PrioritizedCategories AS
(
    SELECT 
        CaseID,
        CaseNumber,
        Category,
        ROW_NUMBER() OVER (
            PARTITION BY CaseNumber 
            ORDER BY 
                CASE Category
                    WHEN 'Hybrid'         THEN 1
                    WHEN 'E2 Expanders'   THEN 2
                    WHEN 'Lab to Lab'     THEN 3
                    WHEN 'Marpe'          THEN 4
                    WHEN 'Metal'          THEN 5
                    WHEN 'Clear'          THEN 6
                    WHEN 'Wire Bending'   THEN 7
                    WHEN 'Airway'         THEN 8
                    ELSE 99  
                END
        ) AS PriorityRank
    FROM ProductCategories
),

/* Case Location – One Row Per CaseNumber (Unique) */
RankedCases AS
(
    SELECT
        ca.CaseNumber                                 AS [Case Number],
        ca.PanNumber                                  AS [Pan Number],
        ct.Task                                       AS [Last Task Completed],
        ct.CompleteDate                               AS [Last Scan Time],
        pc.Category                                   AS [Category],
        CAST(ca.ShipDate AS DATE)                     AS [Ship Date],
        ca.LastLocationID,
        cll.[Description]                             AS [Last Location],
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
    /* Join only the #1 priority category for each case */
    LEFT JOIN PrioritizedCategories AS pc
        ON ca.CaseID = pc.CaseID 
       AND pc.PriorityRank = 1
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