SELECT i.InventoryPK, CustomerID, ROW_NUMBER() OVER (PARTITION BY l.CustomerFK ORDER BY i.InventoryPK) InventoryID 
FROM {{ source('avi', 'Inventories') }} i 
JOIN {{ source('avi', 'Locations') }} l ON l.LocationPK = i.LocationFK 
JOIN {{ ref('MapCustomers') }} c on c.CustomerPK = l.CustomerFK
