WITH CasesWithRankedCategories AS
(
    SELECT
        ca.CaseNumber,
        ca.PanNumber,
        CAST(ca.ShipDate AS DATE) AS ShipDate,
        ca.Status,
        CASE WHEN pr.Category = 'Accessories' THEN 'Other'
             ELSE pr.Category
        END AS Category,
        ROW_NUMBER() OVER (
            PARTITION BY ca.CaseNumber, CAST(ca.ShipDate AS DATE)
            ORDER BY
                CASE WHEN pr.Category = 'Accessories' THEN 'Other'
                     ELSE pr.Category
                END,
                CASE
                    WHEN pr.Category = 'Hybrid'        THEN 1
                    WHEN pr.Category = 'E2 Expanders'  THEN 2
                    WHEN pr.Category = 'Lab to Lab'    THEN 3
                    WHEN pr.Category = 'Marpe'         THEN 4
                    WHEN pr.Category = 'Metal'         THEN 5
                    WHEN pr.Category = 'Clear'         THEN 6
                    WHEN pr.Category = 'Wire Bending'  THEN 7
                    ELSE 99
                END,
                pr.Category DESC
        ) AS rn
    FROM dbo.Cases AS ca
    INNER JOIN dbo.CaseProducts AS cp ON ca.CaseID = cp.CaseID
    INNER JOIN dbo.Products AS pr ON cp.ProductID = pr.ProductID
    WHERE ca.Status IN ('In Production', 'Invoiced')
      AND ca.ShipDate IS NOT NULL
      AND pr.Category <> 'Airway'
)
, FinalAssignment AS
(
    SELECT
        ShipDate,
        CaseNumber,
        PanNumber,
        Status,
        CASE WHEN Category = 'Accessories' THEN 'Other'
             ELSE ISNULL(NULLIF(Category, ''), 'Other')
        END AS Category
    FROM CasesWithRankedCategories
    WHERE rn = 1

    UNION ALL

    -- Add cases that have NO products at all OR no products with any category
    SELECT
        CAST(ca.ShipDate AS DATE) AS ShipDate,
        ca.CaseNumber,
        ca.PanNumber,
        ca.Status,
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
SELECT
    Category,
    Status,
    ShipDate,
    PanNumber,
    CaseNumber
FROM FinalAssignment
ORDER BY ShipDate DESC,
         CASE Category
             WHEN 'Hybrid'        THEN 1
             WHEN 'E2 Expanders'  THEN 2
             WHEN 'Lab to Lab'    THEN 3
             WHEN 'Marpe'         THEN 4
             WHEN 'Metal'         THEN 5
             WHEN 'Clear'         THEN 6
             WHEN 'Wire Bending'  THEN 7
             WHEN 'Other'         THEN 99
         END,
         Category;
