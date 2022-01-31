SELECT DISTINCT c.InventoryPK, c.CollectionPK, CustomerID, map.CollectionID
FROM {{ source('inv', 'Collections') }} c 
JOIN {{ source('avi', 'Inventories') }} i ON i.InventoryPK = c.InventoryPK 
JOIN {{ source('avi', 'Locations') }} l ON l.LocationPK = i.LocationFK
JOIN (
 SELECT l.CustomerFK, CollectionName, CollectionDescription, CollectionHistory, CustomerId, row_number() over(partition by l.CustomerFK order by min(i.inventorypk), CollectionName) CollectionID
 FROM {{ source('inv', 'Collections') }} c 
 JOIN {{ source('avi', 'Inventories') }} i ON i.InventoryPK = c.InventoryPK
 JOIN {{ source('avi', 'Locations') }} l ON l.LocationPK = i.LocationFK
 JOIN {{ ref('MapCustomers') }} customers on customers.CustomerPK = l.CustomerFK
 GROUP BY l.CustomerFK, CollectionName, CollectionDescription, CollectionHistory, CustomerID
) map ON map.CustomerFK = l.CustomerFK AND map.CollectionName = c.CollectionName AND COALESCE(map.CollectionDescription, '') = COALESCE(c.CollectionDescription, '') and COALESCE(map.CollectionHistory, '') = COALESCE(c.CollectionHistory, '')
