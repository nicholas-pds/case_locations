SELECT
    ca.CaseNumber,
    ca.PanNumber,
    ca.DoctorName,
    cu.PracticeName,
    CONCAT(ca.PatientFirst, ' ', ca.PatientLast) AS PatientName,
    CAST(ca.CreateDate AS DATE) AS CreateDate,
    CAST(ca.ShipDate AS DATE) AS ShipDate,
    CAST(ca.HoldDate AS DATE) AS HoldDate,
    ca.HoldStatus,
    ca.HoldReason,
    CASE
        WHEN ca.HoldReason LIKE '%(AFU)%' THEN 'AFU'
        WHEN ca.HoldReason LIKE '%(ZFU)%' THEN 'ZFU'
        WHEN ca.HoldReason LIKE '%(EFU)%' THEN 'EFU'
        ELSE NULL
    END AS [TYPE]
FROM dbo.cases AS ca
LEFT JOIN dbo.Customers AS cu ON ca.CustomerID = cu.CustomerID
WHERE
    ca.[Status] = 'On Hold'
    AND LTRIM(RTRIM(ca.PANNumber)) LIKE '7%'
    AND ca.HoldStatus IN ('Waiting on Scan(s)', 'How to Proceed')
ORDER BY ca.CaseNumber;