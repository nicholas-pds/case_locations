WITH CaseCategories AS (
    SELECT
        cp.CaseID,
        pr.Category,
        ROW_NUMBER() OVER (
            PARTITION BY cp.CaseID
            ORDER BY
                CASE
                    WHEN pr.Category = 'Hybrid'              THEN 1
                    WHEN pr.Category LIKE 'E%Expander%'      THEN 2
                    WHEN pr.Category = 'Lab to Lab'          THEN 3
                    WHEN pr.Category = 'Marpe'               THEN 4
                    WHEN pr.Category = 'Metal'               THEN 5
                    WHEN pr.Category = 'Clear'               THEN 6
                    WHEN pr.Category = 'Wire Bending'        THEN 7
                    WHEN pr.Category = 'Airway'              THEN 8
                    ELSE 99
                END
        ) AS rn
    FROM dbo.CaseProducts AS cp
    INNER JOIN dbo.Products AS pr ON cp.ProductID = pr.ProductID
    WHERE pr.Category IS NOT NULL
)
SELECT
    cat.CreatedBy  AS UserName,
    cat.CreateDate,
    ca.CaseNumber,
    CASE
        WHEN cc.Category = 'Accessories' THEN 'Other'
        ELSE ISNULL(NULLIF(cc.Category, ''), 'Other')
    END AS Category
FROM dbo.CaseAuditTrail AS cat
INNER JOIN dbo.Cases AS ca ON ca.CaseID = cat.CaseID
LEFT JOIN CaseCategories AS cc ON cc.CaseID = cat.CaseID AND cc.rn = 1
WHERE cat.[Type] = 'Accept Remote Case'
  AND cat.CreateDate >= DATEADD(day, -32, CAST(GETDATE() AS DATE))
ORDER BY cat.CreateDate DESC;
