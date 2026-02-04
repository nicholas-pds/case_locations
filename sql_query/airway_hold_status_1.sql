SELECT
    CaseNumber,
    PanNumber,
    DoctorName,
    CONCAT(PatientFirst, ' ', PatientLast) AS PatientName,
    CAST(CreateDate AS DATE) AS CreateDate,
    CAST(ShipDate AS DATE) AS ShipDate,
    CAST(HoldDate AS DATE) AS HoldDate,
    HoldStatus,
    HoldReason,
    CASE
        WHEN HoldReason LIKE '%(AFU)%' THEN 'AFU'
        WHEN HoldReason LIKE '%(ZFU)%' THEN 'ZFU'
        WHEN HoldReason LIKE '%(EFU)%' THEN 'EFU'
        ELSE NULL
    END AS [TYPE]
FROM dbo.cases
WHERE
    [Status] = 'On Hold'
    AND LTRIM(RTRIM(PANNumber)) LIKE '7%'
    AND HoldStatus IN ('Waiting on Scan(s)', 'How to Proceed')
ORDER BY CaseNumber;