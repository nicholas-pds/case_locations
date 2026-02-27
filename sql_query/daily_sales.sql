-- Daily sales for the last 35 calendar days (to capture 30+ calendar days for Chart B)
-- Based on dbo.DailySales view pattern: Invoice ('I') + Sales ('S') rows

-- Invoice data: cases invoiced per day
SELECT
    CAST(c.InvoiceDate AS DATE) AS SalesDate,
    'I' AS Type,
    c.LabName,
    COUNT(c.CaseID) AS NumberOfInvoices,
    SUM(c.TaxableAmount + c.NonTaxableAmount) AS SubTotal,
    SUM(c.SalesDiscount) AS Discount,
    SUM(c.RemakeDiscount) AS RemakeDiscount,
    SUM(c.TotalTax) AS Tax,
    0 AS NewUnits,
    0 AS RemakeUnits
FROM dbo.Cases c WITH (NOLOCK)
WHERE c.Deleted = 0
  AND c.Type = 'D'
  AND COALESCE(c.IsAdjustment, 0) = 0
  AND CAST(c.InvoiceDate AS DATE) >= CAST(DATEADD(day, -35, GETDATE()) AS DATE)
  AND CAST(c.InvoiceDate AS DATE) <= CAST(GETDATE() AS DATE)
GROUP BY CAST(c.InvoiceDate AS DATE), c.LabName

UNION ALL

-- Sales/production data: cases received per day
SELECT
    CAST(c.DateIn AS DATE) AS SalesDate,
    'S' AS Type,
    c.LabName,
    COUNT(DISTINCT c.CaseID) AS NumberOfInvoices,
    SUM(CASE WHEN COALESCE(cp.Remake, '') = '' THEN cp.ExtendedAmount ELSE 0 END) AS SubTotal,
    SUM(cp.SalesDiscount) AS Discount,
    SUM(cp.RemakeDiscount) AS RemakeDiscount,
    0 AS Tax,
    SUM(CASE WHEN COALESCE(cp.Remake, '') = '' THEN cp.Quantity * p.UnitValue ELSE 0 END) AS NewUnits,
    SUM(CASE WHEN COALESCE(cp.Remake, '') <> '' THEN cp.Quantity * p.UnitValue ELSE 0 END) AS RemakeUnits
FROM dbo.CaseProducts cp
LEFT JOIN dbo.Cases c ON c.CaseID = cp.CaseID
LEFT JOIN dbo.Products p ON p.ProductID = cp.ProductID
WHERE c.Type = 'D'
  AND c.Deleted = 0
  AND CAST(c.DateIn AS DATE) >= CAST(DATEADD(day, -35, GETDATE()) AS DATE)
  AND CAST(c.DateIn AS DATE) <= CAST(GETDATE() AS DATE)
  AND c.Status NOT IN ('Cancelled', 'Submitted', 'Sent for TryIn')
GROUP BY CAST(c.DateIn AS DATE), c.LabName

ORDER BY SalesDate DESC, Type;
