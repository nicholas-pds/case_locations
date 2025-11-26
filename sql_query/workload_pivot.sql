WITH CasesWithRankedCategories AS
(
    SELECT 
        ca.CaseNumber,
        CAST(ca.ShipDate AS DATE) AS ShipDate,
        pr.Category,
        ROW_NUMBER() OVER (
            PARTITION BY ca.CaseNumber, CAST(ca.ShipDate AS DATE)
            ORDER BY 
                CASE pr.Category
                    WHEN 'E2 Expanders'   THEN 1
                    WHEN 'Lab to Lab'     THEN 2
                    WHEN 'Marpe'          THEN 3
                    WHEN 'Metal'          THEN 4
                    WHEN 'Clear'          THEN 5
                    WHEN 'Wire Bending'   THEN 6
                    WHEN 'Hybrid'         THEN 7
                    WHEN 'Airway'         THEN 8
                    ELSE 99                                -- Lowest priority
                END,
                pr.Category DESC  -- Just in case of ties (unlikely)
        ) AS rn
    FROM dbo.Cases AS ca
    INNER JOIN dbo.CaseProducts AS cp ON ca.CaseID = cp.CaseID
    INNER JOIN dbo.Products AS pr ON cp.ProductID = pr.ProductID
    WHERE ca.Status = 'In Production'
      AND ca.ShipDate IS NOT NULL
      -- Remove the restrictive IN clause so we can catch NULLs and unknown categories
      -- AND pr.Category IN (...)   <-- removed on purpose
)
, FinalAssignment AS
(
    SELECT 
        ShipDate,
        CaseNumber,
        ISNULL(
            NULLIF(Category, ''), 
            'Other'
        ) AS FinalCategory
    FROM CasesWithRankedCategories
    WHERE rn = 1

    UNION ALL

    -- Add cases that have NO products at all OR no products with any category
    SELECT 
        CAST(ca.ShipDate AS DATE) AS ShipDate,
        ca.CaseNumber,
        'Other' AS FinalCategory
    FROM dbo.Cases AS ca
    WHERE ca.Status = 'In Production'
      AND ca.ShipDate IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 
          FROM dbo.CaseProducts cp 
          INNER JOIN dbo.Products pr ON cp.ProductID = pr.ProductID
          WHERE cp.CaseID = ca.CaseID
      )
)
SELECT
    FinalCategory AS Category,
    ShipDate,
    COUNT(*) AS CaseCount
FROM FinalAssignment
GROUP BY FinalCategory, ShipDate
ORDER BY ShipDate DESC, 
         CASE FinalCategory 
             WHEN 'E2 Expanders' THEN 1
             WHEN 'Lab to Lab'   THEN 2
             WHEN 'Marpe'        THEN 3
             WHEN 'Metal'        THEN 4
             WHEN 'Clear'        THEN 5
             WHEN 'Wire Bending' THEN 6
             WHEN 'Hybrid'       THEN 7
             WHEN 'Airway'       THEN 8
             WHEN 'Other'       THEN 99
         END,
         FinalCategory;