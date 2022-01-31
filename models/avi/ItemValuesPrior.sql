select map.CustomerId, map.ItemID, inv.InventoryTypeFK AS ValueTypeId, i.PriorValue
FROM {{ source('inv', 'Items') }} i
join {{ source('avi', 'Inventories') }} inv on inv.InventoryPK = i.InventoryPK
join {{ ref('MapItems') }} map on map.InventoryPK = i.InventoryPK and map.ItemPK = i.ItemPK
join {{ ref('MapInventories') }} mapi on mapi.InventoryPK = i.InventoryPK
where COALESCE(i.PriorValue, 0) <> 0
