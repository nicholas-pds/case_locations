WITH ProductCategories AS
(
    SELECT 
        ca.CaseID,
        ca.CaseNumber,
        pr.Category
    FROM dbo.Cases AS ca
    LEFT JOIN dbo.Caseproducts AS cp   ON ca.CaseID = cp.CaseID
    LEFT JOIN dbo.Products AS pr       ON cp.ProductID = pr.ProductID
    WHERE ca.Status = 'In Production'
      AND pr.Category IS NOT NULL
)

SELECT 
    pc.Category,
    CAST(ca.ShipDate AS DATE) AS ShipDate,
    COUNT(DISTINCT ca.CaseNumber) AS CaseCount
FROM dbo.Cases AS ca
INNER JOIN ProductCategories pc ON ca.CaseID = pc.CaseID
WHERE ca.Status = 'In Production'
  AND ca.ShipDate IS NOT NULL
GROUP BY 
    pc.Category,
    CAST(ca.ShipDate AS DATE)
ORDER BY 
    ShipDate DESC, 
    pc.Category;