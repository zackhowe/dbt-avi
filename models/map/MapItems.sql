SELECT DISTINCT items.InventoryPK
, items.ItemPK
, mc.CustomerId
, map.ItemID
FROM {{ source('inv', 'Items') }} items 
JOIN {{ source('avi', 'Inventories') }} i ON i.InventoryPK = items.InventoryPK 
JOIN {{ source('avi', 'Locations') }} l ON l.LocationPK = i.LocationFK
LEFT JOIN {{ ref ('MapImageFiles') }} map_img on map_img.InventoryPK = items.InventoryPK AND map_img.ImageFilePK = items.PrimaryImageFK
LEFT JOIN {{ ref('MapItemFields') }} fields ON fields.InventoryPK = items.InventoryPK AND fields.ItemPK = items.ItemPK
JOIN {{ ref('MapItemsAgg') }} map ON map.CustomerFK = l.CustomerFK AND map.ItemPK = items.ItemPK
JOIN {{ ref('MapCustomers') }} mc on mc.CustomerPK = map.CustomerFK
AND ((map.ItemCategoryFK = items.ItemCategoryFK) OR (map.ItemCategoryFK IS NULL AND items.ItemCategoryFK IS NULL))
AND ((map.ItemType = items.ItemType) OR (map.ItemType IS NULL AND items.ItemType IS NULL))
AND ((map.ItemDescription = items.ItemDescription) OR (map.ItemDescription IS NULL AND items.ItemDescription IS NULL))
AND ((map.ItemDescriptionReadOnly = items.ItemDescriptionReadOnly) OR (map.ItemDescriptionReadOnly IS NULL AND items.ItemDescriptionReadOnly IS NULL))
AND ((map.ItemDescriptionOverride = items.ItemDescriptionOverride) OR (map.ItemDescriptionOverride IS NULL AND items.ItemDescriptionOverride IS NULL))
AND ((map.ItemHistoryId = items.ItemHistoryId) OR (map.ItemHistoryId IS NULL AND items.ItemHistoryId IS NULL))
AND ((map.ItemQuantity = items.ItemQuantity) OR (map.ItemQuantity IS NULL AND items.ItemQuantity IS NULL))
AND ((map.DHeight = items.DHeight) OR (map.DHeight IS NULL AND items.DHeight IS NULL))
AND ((map.DWidth = items.DWidth) OR (map.DWidth IS NULL AND items.DWidth IS NULL))
AND ((map.DDepth = items.DDepth) OR (map.DDepth IS NULL AND items.DDepth IS NULL))
AND ((map.DFrameHeight = items.DFrameHeight) OR (map.DFrameHeight IS NULL AND items.DFrameHeight IS NULL))
AND ((map.DFrameWidth = items.DFrameWidth) OR (map.DFrameWidth IS NULL AND items.DFrameWidth IS NULL))
AND ((map.DUnitOfMeasure = items.DUnitOfMeasure) OR (map.DUnitOfMeasure IS NULL AND items.DUnitOfMeasure IS NULL))
AND ((map.PurchaseDt = items.PurchaseDt) OR (map.PurchaseDt IS NULL AND items.PurchaseDt IS NULL))
AND ((map.GivenValue = items.GivenValue) OR (map.GivenValue IS NULL AND items.GivenValue IS NULL))
AND ((map.AppraisalProvenance = items.AppraisalProvenance) OR (map.AppraisalProvenance IS NULL AND items.AppraisalProvenance IS NULL))
AND ((map.Notes = items.Notes) OR (map.Notes IS NULL AND items.Notes IS NULL))
AND ((map.ItemFields = fields.ItemFields) OR (map.ItemFields IS NULL AND fields.ItemFields IS NULL))
AND ((map.ImageFileID = map_img.ImageFileID) OR (map.ImageFileID IS NULL AND map_img.ImageFileID IS NULL))
