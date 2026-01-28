SELECT 
    LastLocation,
    CAST(ShipDate AS DATE) AS ShipDate,
    CaseNumber,
    PanNumber,
    CAST(CreateDate AS DATE) AS CreateDate,
    [Status],
    LastTaskCompleted,
    LastScanTime
FROM (
    SELECT
        ca.CaseNumber,
        ca.PanNumber,
        ca.ShipDate,
        ca.CreateDate,
        ca.[Status],
        cll.[Description] AS LastLocation,
        ct.Task AS LastTaskCompleted,
        ct.CompleteDate AS LastScanTime,
        ROW_NUMBER() OVER (
            PARTITION BY ca.CaseNumber 
            ORDER BY ct.CompleteDate DESC, ct.CaseID DESC
        ) AS rn
    FROM dbo.Cases AS ca
    LEFT JOIN dbo.CaseTasks AS ct 
        ON ca.CaseID = ct.CaseID
    LEFT JOIN dbo.CaseLogLocations AS cll 
        ON ca.LastLocationID = cll.ID
) AS BaseData
WHERE rn = 1 
  AND LastLocation IN (
    'New Cases',
    'New Cases How to Proceed',
    'New Cases Waiting For Scans',
    'Email Plan Case',
    'Email Follow Up',
    'Zoom Set Up',
    'Zoom Consult',
    'Zoom Export Needed',
    'Zoom Waiting Approval'
  )
ORDER BY LastLocation, ShipDate ASC;