SELECT CustomerFK
, min(items.InventoryPK) MinInventoryPK
, items.ItemPK
, items.ItemCategoryFK
, ItemType
, ItemDescription
, ItemDescriptionReadOnly
, ItemDescriptionOverride
, MIN(ih.ItemHistoryId) AS ItemHistoryId
, ItemQuantity
, DHeight
, DWidth
, DDepth
, DFrameHeight
, DFrameWidth
, DUnitOfMeasure
, PurchaseDt
, GivenValue
, AppraisalProvenance
, items.Notes
, fields.ItemFields
, map.ImageFileID
, ROW_NUMBER() OVER (PARTITION BY CustomerFK ORDER BY min(items.InventoryPK), items.ItemPK) ItemID
FROM {{ source('inv', 'Items') }} items
JOIN {{ source('avi', 'Inventories') }} i ON i.InventoryPK = items.InventoryPK 
JOIN {{ source('avi', 'Locations') }} l ON l.LocationPK = i.LocationFK 
LEFT JOIN {{ ref('MapImageFiles') }} map ON map.InventoryPK = items.InventoryPK AND map.ImageFilePK = items.PrimaryImageFK
LEFT JOIN {{ ref('MapItemFields') }} fields ON fields.InventoryPK = items.InventoryPK AND fields.ItemPK = items.ItemPK
LEFT JOIN {{ ref('ItemHistories') }} ih ON ih.ItemHistoryDescription = ItemHistory AND ih.ItemCategoryId = items.ItemCategoryFK
GROUP BY CustomerFK
, items.ItemPK
, items.ItemCategoryFK
, ItemType
, ItemDescription
, ItemDescriptionReadOnly
, ItemDescriptionOverride
, ItemHistory
, ItemQuantity
, DHeight
, DWidth
, DDepth
, DFrameHeight
, DFrameWidth
, DUnitOfMeasure
, PurchaseDt
, GivenValue
, AppraisalProvenance
, items.Notes
, fields.ItemFields
, map.ImageFileID
