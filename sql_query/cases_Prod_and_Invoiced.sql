WITH AdjustedCases AS (
    SELECT
        ca.Status,
        -- Calculate the potentially adjusted ShipDate
        CASE
            -- Condition to check: PanNumber starts with 'R' or 'r' AND is less than 4 characters long
            WHEN (ca.PanNumber LIKE 'R%' OR ca.PanNumber LIKE 'r%')
                 AND LEN(ca.PanNumber) < 4
            THEN
                -- If the condition is met, calculate the previous business day
                CAST(
                    CASE
                        -- If the original ShipDate is a Monday (DatePart(weekday, ...) = 2)
                        -- the previous business day is Friday (3 days prior)
                        WHEN DATEPART(WEEKDAY, ca.ShipDate) = 2 THEN DATEADD(day, -3, ca.ShipDate)
                        -- If the original ShipDate is a Sunday (DatePart(weekday, ...) = 1)
                        -- the previous business day is Friday (2 days prior)
                        WHEN DATEPART(WEEKDAY, ca.ShipDate) = 1 THEN DATEADD(day, -2, ca.ShipDate)
                        -- For all other days (Tuesday-Saturday), the previous business day is simply 1 day prior
                        ELSE DATEADD(day, -1, ca.ShipDate)
                    END
                AS DATE)
            -- If the PanNumber condition is NOT met, use the original ShipDate
            ELSE CAST(ca.ShipDate AS DATE)
        END AS AdjustedShipDate
    FROM dbo.Cases ca
    WHERE ca.Status IN ('In Production', 'Invoiced')
      AND ca.ShipDate IS NOT NULL
      -- Note: The date range check should use the original ShipDate
      AND ca.ShipDate >  CAST(DATEADD(day, -5, GETDATE()) AS DATE)
      AND ca.ShipDate <  CAST(DATEADD(day, 11, GETDATE()) AS DATE)
)
SELECT
    ac.Status AS TypeCount,
    ac.AdjustedShipDate AS ShipDate,
    COUNT(*) AS Count
FROM AdjustedCases ac
GROUP BY
    ac.Status,
    ac.AdjustedShipDate
ORDER BY
    ShipDate DESC,
    CASE WHEN ac.Status = 'In Production' THEN 1 ELSE 2 END;
