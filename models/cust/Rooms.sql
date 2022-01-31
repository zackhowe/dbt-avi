SELECT map.CustomerID
, map.LocationID
, map.RoomID
, CAST(1 as Bit) AS ServerIdFlag
, CAST(0 as Bit) AS SoftDeleteFlag
, CAST(null AS TIMESTAMP) AS RowVersion
, max(r.timestamp) AS CreateUtcDttm
, RoomName
, min(img1.ImageFileID) PrimaryImageFileID
, min(img2.ImageFileID) SecondaryImageFileID
FROM {{ source('inv', 'Rooms') }} r 
join {{ ref('MapRooms') }} map ON map.InventoryPK = r.InventoryPK AND map.RoomPK = r.RoomPK 
join {{ source('avi', 'Inventories') }} i ON i.InventoryPK = r.InventoryPK
left join {{ ref('MapImageFiles') }} img1 ON img1.InventoryPK = r.InventoryPK AND img1.ImageFilePK = r.PrimaryImageFK
left join {{ ref('MapImageFiles') }} img2 ON img2.InventoryPK = r.InventoryPK AND img2.ImageFilePK = r.SecondaryImageFK
GROUP BY map.CustomerID, map.LocationId, map.RoomID, RoomName
