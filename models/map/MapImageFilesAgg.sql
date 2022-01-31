{{ config(materialization="ephemeral") }}

SELECT l.CustomerFK, img.ImageFileName /*Size, Hash */
FROM {{ source('inv', 'ImageFiles') }} img 
JOIN {{ source('avi', 'Inventories') }} i ON i.InventoryPK = img.InventoryPK 
JOIN {{ source('avi', 'Locations') }} l ON l.LocationPK = i.LocationFK
GROUP BY l.CustomerFK, img.ImageFileName
