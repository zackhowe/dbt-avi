SELECT map.CustomerId, map.ItemID, min(UserGroupFK) AS UserGroupID
FROM {{ source('inv', 'Items') }}  i
JOIN {{ ref('MapItems') }} map ON map.InventoryPK = i.InventoryPK AND map.ItemPK = i.ItemPK
WHERE UserGroupFK > 0
GROUP BY map.CustomerId, map.ItemID
