SELECT CustomerID
, ImageFileID
, cast(1 AS Bit) AS ServerIdFlag
, cast(0 AS Bit) AS SoftDeleteFlag
, [img].NetFxTicksToDateTime(CreateUtcTick) CreateUtcDttm
, CreateUtcTick
, ImageFileName
, OriginalFileName
, Cast(0 AS TinyInt) AS ReferenceCount
, CAST(0 AS BigInt) AS FileSize
, CAST(null AS NVARCHAR(50)) Hash
FROM {{ ref('MapImageFiles') }}
