SELECT
    cth.CompletedBY AS [Completed by name],
    cth.Task,
    ca.CaseNumber,
    cth.completeDate AS [completeDate]
FROM dbo.CaseTasksHistory AS cth
INNER JOIN dbo.cases AS ca
    ON ca.CaseID = cth.CaseID
WHERE cth.completeDate >= CAST(DATEADD(day, -14, GETDATE()) AS DATE)
  AND cth.completeDate < CAST(DATEADD(day, 1, GETDATE()) AS DATE)
  AND cth.Task IN ('3dd', '3dcf')
ORDER BY [completeDate] ASC;
