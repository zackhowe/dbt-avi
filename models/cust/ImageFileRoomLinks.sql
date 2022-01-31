select map.CustomerId, map.ImageFileID, LocationID, RoomID
FROM {{ source('inv', 'ImageFiles') }} img
JOIN {{ ref('MapImageFiles') }} map ON map.InventoryPK = img.InventoryPK AND map.ImageFilePK = img.ImageFilePK
JOIN {{ ref('MapRooms') }} r ON r.InventoryPK = img.InventoryPK AND r.RoomPK = img.RoomFK
