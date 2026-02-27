-- Monthly invoice revenue aggregation for last 19 months
-- (19 months so 3-month rolling average has data at month 1 of the 18-month window)
SELECT
    YEAR(c.InvoiceDate)  AS SalesYear,
    MONTH(c.InvoiceDate) AS SalesMonth,
    COUNT(c.CaseID)      AS NumberOfInvoices,
    SUM(c.TaxableAmount + c.NonTaxableAmount) AS SubTotal
FROM dbo.Cases c WITH (NOLOCK)
WHERE c.Deleted = 0
  AND c.Type = 'D'
  AND COALESCE(c.IsAdjustment, 0) = 0
  AND c.InvoiceDate >= DATEADD(month, -19, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))
  AND c.InvoiceDate <  DATEADD(month,   1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))
GROUP BY YEAR(c.InvoiceDate), MONTH(c.InvoiceDate)
ORDER BY SalesYear, SalesMonth;
