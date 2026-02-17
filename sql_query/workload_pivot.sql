WITH CasesWithRankedCategories AS
(
    SELECT
        ca.CaseNumber,
        ca.PanNumber,
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
)
, FinalAssignment AS
(
    SELECT
        ShipDate,
        CaseNumber,
        PanNumber,
        ISNULL(
            NULLIF(Category, ''),
            'Other'
        ) AS Category
    FROM CasesWithRankedCategories
    WHERE rn = 1

    UNION ALL

    -- Add cases that have NO products at all OR no products with any category
    SELECT
        CAST(ca.ShipDate AS DATE) AS ShipDate,
        ca.CaseNumber,
        ca.PanNumber,
        'Other' AS Category
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
    Category,
    ShipDate,
    PanNumber,
    CaseNumber
FROM FinalAssignment
ORDER BY ShipDate DESC,
         CASE Category
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
         Category;
