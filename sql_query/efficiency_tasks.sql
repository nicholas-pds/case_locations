SELECT cth.CompletedBY,
       ca.CaseNumber,
       cth.completeDate,
       cth.task,
       cth.rejected,
       cth.Quantity,
       cth.CaseProductID,
       dur.Duration
FROM dbo.CaseTasksHistory AS cth
INNER JOIN dbo.cases AS ca
    ON ca.CaseID = cth.CaseID
OUTER APPLY (
    SELECT TOP 1 ct.Duration
    FROM dbo.casetasks AS ct
    WHERE ct.CaseID = cth.CaseID AND ct.task = cth.task
) AS dur
WHERE cth.completeDate >= ?
  AND cth.completeDate < ?
