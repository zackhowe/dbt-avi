select CustomerID
, ItemID
, 1 AS ServerIdFlag
, CAST(null  AS TIMESTAMP) AS RowVersion
, CAST('2000-01-01 00:00:00' AS DateTime) AS CreateUtcDttm
, CAST('2000-01-01 00:00:00' AS DateTime) AS LastUpdateUtcDttm
, CAST(0 AS TinyInt) AS ItemDeletionTypeID
, ItemCategoryFK AS ItemCategoryID
, ItemType
, ItemDescription
, ItemDescriptionReadOnly
, ItemDescriptionOverride
, ItemHistoryId
, ItemQuantity
, DHeight AS DimensionHeight
, DWidth AS DimensionWidth
, DDepth AS DimensionDepth
, DFrameHeight AS DimensionFrameHeight
, DFrameWidth AS DimensionFrameWidth
, DUnitOfMeasure AS DimensionUnitOfMeasure
, ImageFileID AS PrimaryImageFileID
, PurchaseDt
, GivenValue AS PurchasePrice
, 0 AS HasReceipt
, CAST(null AS DateTime) AS OriginalAppraisalDt
, CAST(null AS Decimal(10,2)) AS OriginalAppraisalValue
, AppraisalProvenance 
, ItemFields
FROM {{ ref('MapItemsAgg') }} map
JOIN {{ ref('MapCustomers') }} mc ON mc.CustomerPK = map.CustomerFK
