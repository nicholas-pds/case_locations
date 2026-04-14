WITH CasesWithRankedCategories AS (
    SELECT
        ca.CaseID,
        ca.CaseNumber,
        pr.Category,
        ROW_NUMBER() OVER (
            PARTITION BY ca.CaseNumber, CAST(ca.ShipDate AS DATE)
            ORDER BY
                CASE pr.Category
                    WHEN 'Hybrid'        THEN 1
                    WHEN 'E² Expanders'  THEN 2
                    WHEN 'Lab to Lab'    THEN 3
                    WHEN 'Marpe'         THEN 4
                    WHEN 'Metal'         THEN 5
                    WHEN 'Clear'         THEN 6
                    WHEN 'Wire Bending'  THEN 7
                    WHEN 'Airway'        THEN 8
                    ELSE 99
                END,
                pr.Category DESC
        ) AS rn
    FROM dbo.Cases AS ca
    INNER JOIN dbo.CaseProducts AS cp ON ca.CaseID = cp.CaseID
    INNER JOIN dbo.Products AS pr ON cp.ProductID = pr.ProductID
    WHERE ca.Status IN ('In Production', 'Invoiced')
      AND ca.ShipDate IS NOT NULL
),
FinalCategoryAssignment AS (
    SELECT
        CaseNumber,
        CASE 
            WHEN Category = 'Airway' THEN 'MARPE'
            WHEN Category = 'Accessories' THEN 'Other'
            ELSE ISNULL(NULLIF(Category, ''), 'Other')
        END AS Category
    FROM CasesWithRankedCategories
    WHERE rn = 1
    UNION ALL
    SELECT
        ca.CaseNumber,
        'Other' AS Category
    FROM dbo.Cases AS ca
    WHERE ca.Status IN ('In Production', 'Invoiced')
      AND ca.ShipDate IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM dbo.CaseProducts cp
          INNER JOIN dbo.Products pr ON cp.ProductID = pr.ProductID
          WHERE cp.CaseID = ca.CaseID
      )
)
-- The Aggregated Result
SELECT 
    cat.CreatedBy AS UserName,
    CAST(cat.CreateDate AS DATE) AS AuditDate,
    fca.Category,
    COUNT(ca.CaseNumber) AS CaseCount
FROM dbo.CaseAuditTrail AS cat
LEFT JOIN dbo.cases AS ca ON ca.caseid = cat.CaseID
LEFT JOIN FinalCategoryAssignment AS fca ON ca.CaseNumber = fca.CaseNumber
WHERE cat.[Type] IN ('Accept Remote Case') 
  AND cat.CreateDate >= DATEADD(day, -30, GETDATE())
GROUP BY 
    cat.CreatedBy,
    CAST(cat.CreateDate AS DATE), 
    fca.Category
ORDER BY 
    AuditDate DESC, 
    UserName ASC, 
    CaseCount DESC;