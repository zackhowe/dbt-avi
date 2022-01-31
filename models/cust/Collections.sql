SELECT map.CustomerID
, map.CollectionID
, CAST(1 AS Bit) AS ServerIdFlag
, CAST(0 AS Bit) As SoftDeleteFlag
, CAST(null AS TIMESTAMP) AS RowVersion
, max(timestamp) AS CreateUtcDttm
, min(ImageFileID) AS ImageFileID
, CollectionName
, CollectionDescription
, CollectionHistory
FROM {{ source('inv', 'Collections') }} c 
JOIN {{ ref('MapCollections') }} map ON map.InventoryPK = c.InventoryPK AND map.CollectionPK = c.CollectionPK 
LEFT JOIN {{ ref('MapImageFiles') }} img ON img.InventoryPK = c.InventoryPK AND img.ImageFilePK = c.PrimaryImageFK
GROUP BY map.CustomerID, map.CollectionID, CollectionName, CollectionDescription, CollectionHistory
