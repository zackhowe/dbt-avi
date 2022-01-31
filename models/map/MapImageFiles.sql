SELECT InventoryPK
, ImageFilePK
, CustomerId
, ImageFileID
, CreateUtcTick
, [img].[EncodeFileName](CreateUtcTick) + [img].[GetFileExtension](ImageFileName) ImageFileName
, ImageFileName OriginalFileName
FROM (
SELECT i.InventoryPK
, img.ImageFilePK
, CustomerId
, ROW_NUMBER() OVER (PARTITION BY l.CustomerFK ORDER BY img.InventoryPK, img.ImageFileName) ImageFileID
, img.ImageFileName
, (([img].[NetFxTicksFromDateTime2](i.CreateDt) / 10) + ROW_NUMBER() OVER (PARTITION BY map.CustomerFK ORDER BY i.InventoryPK, img.ImageFilePK)) * 10 CreateUtcTick
FROM {{ source('inv', 'ImageFiles') }} img 
JOIN {{ source('avi', 'Inventories') }} i ON i.InventoryPK = img.InventoryPK 
JOIN {{ source('avi', 'Locations') }} l ON l.LocationPK = i.LocationFK
JOIN {{ ref('MapImageFilesAgg') }} map ON map.CustomerFK = l.CustomerFK AND map.ImageFileName = img.ImageFileName
JOIN {{ ref('MapCustomers') }} mc on mc.CustomerPK = l.CustomerFK
) tmp_map
