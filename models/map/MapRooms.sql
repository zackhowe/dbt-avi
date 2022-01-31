SELECT r.InventoryPK, r.RoomPK, CustomerID, map.LocationID, map.RoomID
FROM {{ source('inv', 'Rooms') }} r 
JOIN {{ source('avi', 'Inventories') }} i ON i.InventoryPK = r.InventoryPK 
JOIN {{ source('avi', 'Locations') }} l ON l.LocationPK = i.LocationFK
JOIN (
 SELECT map.CustomerID, map.LocationID, l.CustomerFK, l.LocationPK, RoomName, row_number() over(partition by l.CustomerFK,l.LocationPK order by RoomName) RoomID
 FROM {{ source('inv', 'Rooms') }} r 
 JOIN {{ source('avi', 'Inventories') }} i ON i.InventoryPK = r.InventoryPK 
 JOIN {{ source('avi', 'Locations') }} l ON l.LocationPK = i.LocationFK
 JOIN {{ ref('MapLocations') }} map ON map.LocationPK = l.LocationPK
 GROUP BY map.CustomerID, map.LocationID, l.CustomerFK, l.LocationPK, RoomName
) map ON map.CustomerFK = l.CustomerFK AND map.LocationPK = l.LocationPK AND map.RoomName = r.RoomName;
