

/* PROJECT: Global Operations & Infrastructure Master Dataset
   AUTHOR: [His Name/GitHub Handle]
   DESCRIPTION:
   This script aggregates multi-source infrastructure data into a single
   reporting layer. It utilizes CTEs and Window Functions to handle
   versioning, historical tracking, and future capacity forecasting.
*/

WITH CurrentAssetStatus AS (
    SELECT
        s.AssetID,
        s.StatusTypeID,
        s.EffectiveDate,
        ROW_NUMBER() OVER (
            PARTITION BY s.AssetID
            ORDER BY s.EffectiveDate DESC
        ) AS RowNum
    FROM dbo.AssetHistory s
    WHERE s.EffectiveDate <= CAST(GETDATE() AS date)
),
LatestAnnualMetrics AS (
    SELECT
        m.AssetID,
        m.[Year] AS ReportingYear,
        m.TotalArea,
        m.CapitalInvestment,
        m.Headcount,
        ROW_NUMBER() OVER (
            PARTITION BY m.AssetID
            ORDER BY m.[Year] DESC
        ) AS RowNum
    FROM dbo.AssetMetrics m
    WHERE m.[Year] <= YEAR(GETDATE())
),
OperatingCapacity AS (
    SELECT
        c.AssetID,
        c.[Year] AS CapacityYear,
        c.OutputVolume AS CurrentOutput,
        c.UnitTypeID,
        ROW_NUMBER() OVER (
            PARTITION BY c.AssetID
            ORDER BY c.[Year] DESC
        ) AS RowNum
    FROM dbo.AssetMetrics c
    WHERE c.[Year] <= YEAR(GETDATE())
      AND c.OutputVolume IS NOT NULL
),
PlannedExpansion AS (
    SELECT
        e.AssetID,
        e.[Year] AS ExpansionYear,
        e.OutputVolume AS PlannedOutput,
        ROW_NUMBER() OVER (
            PARTITION BY e.AssetID
            ORDER BY e.[Year] ASC
        ) AS RowNum
    FROM dbo.AssetMetrics e
    JOIN dbo.Assets a ON a.AssetID = e.AssetID
    WHERE e.[Year] > YEAR(GETDATE())
      AND e.OutputVolume IS NOT NULL
      AND a.IsScalable = 1
)

SELECT
    a.AssetID,
    a.AssetName,
    acc.ClientName,
    at.CategoryName AS AssetType,
    a.PostalCode,
    a.Region,
    a.Latitude,
    a.Longitude,
    CAST(CASE WHEN a.IsScalable = 1 THEN 1 ELSE 0 END AS bit) AS ExpansionEligible,
    -- Hierarchy Mapping
    h1.Label AS Division,
    h2.Label AS Department,
    h3.Label AS ProductLine,
    -- Status and Capacity Logic
    st.StatusName AS CurrentStatus,
    cas.EffectiveDate AS LastStatusUpdate,
    oc.CurrentOutput,
    u.UnitName AS MeasurementUnits,
    -- Forecasting Joins
    ex1.ExpansionYear AS NextPhaseYear,
    ex1.PlannedOutput AS NextPhaseVolume,
    -- Financial Metrics
    am.TotalArea AS SquareFootage,
    am.CapitalInvestment AS [TotalInvestment($B)],
    am.Headcount AS TotalStaff
FROM dbo.Assets a
LEFT JOIN dbo.Clients acc ON acc.ClientID = a.ClientID
LEFT JOIN dbo.AssetCategories at ON at.CategoryID = a.CategoryID
LEFT JOIN dbo.AssetHierarchy h3 ON h3.NodeID = a.NodeID
LEFT JOIN dbo.AssetHierarchy h2 ON h2.NodeID = h3.ParentNodeID
LEFT JOIN dbo.AssetHierarchy h1 ON h1.NodeID = h2.ParentNodeID
LEFT JOIN CurrentAssetStatus cas ON cas.AssetID = a.AssetID AND cas.RowNum = 1
LEFT JOIN dbo.StatusTypes st ON st.StatusID = cas.StatusTypeID
LEFT JOIN OperatingCapacity oc ON oc.AssetID = a.AssetID AND oc.RowNum = 1
LEFT JOIN dbo.UnitTypes u ON u.UnitID = oc.UnitTypeID
LEFT JOIN PlannedExpansion ex1 ON ex1.AssetID = a.AssetID AND ex1.RowNum = 1
LEFT JOIN LatestAnnualMetrics am ON am.AssetID = a.AssetID AND am.RowNum = 1;
